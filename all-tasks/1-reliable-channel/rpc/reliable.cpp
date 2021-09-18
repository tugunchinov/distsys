#include <rpc/reliable.hpp>
#include <rpc/errors.hpp>

// Futures
#include <await/futures/core/future.hpp>

// Fibers
#include <await/fibers/sync/future.hpp>
#include <await/fibers/core/api.hpp>

// Logging
#include <timber/log.hpp>

#include <wheels/support/compiler.hpp>

#include <chrono>

using await::fibers::Await;
using await::futures::Future;
using await::futures::Promise;
using wheels::Result;

namespace rpc {

//////////////////////////////////////////////////////////////////

class ReliableChannelOnFutures : public IChannel {
  struct ParameterPack {
    Method method;
    Message request;
    CallOptions options;
  };

  class Retrier : public std::enable_shared_from_this<Retrier> {
   public:
    Retrier(IChannelPtr fair_loss, const Backoff::Params backoff_params,
            IRuntime* runtime, ParameterPack pack, Promise<Message>&& p)
        : fair_loss_(fair_loss),
          backoff_(backoff_params),
          runtime_(runtime),
          pack_(pack),
          p_(std::move(p)) {
    }

    void HandleResult(Result<Message>&& result) {
      if (result.HasError() && IsRetriableError(result.GetErrorCode())) {
        if (pack_.options.stop_advice.StopRequested()) {
          std::move(p_).SetError(Cancelled());
        } else {
          RetryWithDelay();
        }
      } else {
        std::move(p_).Set(std::move(result));
      }
    }

   private:
    void RetryWithDelay() {
      runtime_->Timers()
          ->After(backoff_())
          .Via(runtime_->Executor())
          .Subscribe([this, self = shared_from_this()](Result<void>&&) {
            fair_loss_->Call(pack_.method, pack_.request, pack_.options)
                .Via(runtime_->Executor())
                .Subscribe([this, seflf = shared_from_this()](
                               Result<Message>&& result) {
                  HandleResult(std::move(result));
                });
          });
    }

   private:
    IChannelPtr fair_loss_;
    Backoff backoff_;
    IRuntime* runtime_;
    ParameterPack pack_;
    Promise<Message> p_;
  };

 public:
  ReliableChannelOnFutures(IChannelPtr fair_loss,
                           Backoff::Params backoff_params, IRuntime* runtime)
      : fair_loss_(std::move(fair_loss)),
        backoff_params_(backoff_params),
        runtime_(runtime),
        logger_("Reliable", runtime->Log()) {
  }

  Future<Message> Call(Method method, Message request,
                       CallOptions options) override {
    WHEELS_UNUSED(backoff_params_);
    WHEELS_UNUSED(runtime_);

    LOG_INFO("Call({}, {}) started", method, request);

    auto [f, p] = await::futures::MakeContract<Message>();

    ParameterPack pack = {method, request, options};
    auto retrier = std::make_shared<Retrier>(fair_loss_, backoff_params_,
                                             runtime_, pack, std::move(p));

    fair_loss_->Call(method, request, options)
        .Via(runtime_->Executor())
        .Subscribe([retrier](Result<Message>&& result) {
          retrier->HandleResult(std::move(result));
        });

    return std::move(f);
  }

 private:
  IChannelPtr fair_loss_;
  const Backoff::Params backoff_params_;
  IRuntime* runtime_;
  timber::Logger logger_;
};

//////////////////////////////////////////////////////////////////////

class ReliableChannelOnFibers : public IChannel {
 public:
  ReliableChannelOnFibers(IChannelPtr fair_loss, Backoff::Params backoff_params,
                          IRuntime* runtime)
      : fair_loss_(std::move(fair_loss)),
        backoff_params_(backoff_params),
        runtime_(runtime),
        logger_("Reliable", runtime->Log()) {
  }

  Future<Message> Call(Method method, Message request,
                       CallOptions options) override {
    WHEELS_UNUSED(backoff_params_);
    WHEELS_UNUSED(runtime_);

    LOG_INFO("Call({}, {}) started", method, request);

    auto [f, p] = await::futures::MakeContract<Message>();

    await::fibers::Start(
        runtime_->Executor(), [fair_loss = fair_loss_, runtime = runtime_,
                               backoff = Backoff(backoff_params_), method,
                               request, options, p = std::move(p)]() mutable {
          auto result = Await(fair_loss->Call(method, request, options));

          while (result.HasError() && IsRetriableError(result.GetErrorCode())) {
            if (options.stop_advice.StopRequested()) {
              std::move(p).SetError(Cancelled());
              break;
            }

            Await(runtime->Timers()->After(backoff())).ExpectOk();
            result = Await(fair_loss->Call(method, request, options));
          }

          if (result.IsOk()) {
            std::move(p).Set(std::move(result));
          }
        });

    return std::move(f);
  }

 private:
  IChannelPtr fair_loss_;
  const Backoff::Params backoff_params_;
  IRuntime* runtime_;
  timber::Logger logger_;
};

//////////////////////////////////////////////////////////////////////

// using ReliableChannel = ReliableChannelOnFutures;
using ReliableChannel = ReliableChannelOnFibers;

IChannelPtr MakeReliableChannel(IChannelPtr fair_loss,
                                Backoff::Params backoff_params,
                                IRuntime* runtime) {
  return std::make_shared<ReliableChannel>(std::move(fair_loss), backoff_params,
                                           runtime);
}

}  // namespace rpc