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

class ReliableChannelOnCallbacks : public IChannel {
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

    void Start() {
      Call().Subscribe([self = shared_from_this()](Result<Message>&& result) {
        self->HandleResult(std::move(result));
      });
    }

   private:
    bool StopRequested() {
      return pack_.options.stop_advice.StopRequested();
    }

    bool NeedRetry(const Result<Message>& result) {
      return result.HasError() && IsRetriableError(result.GetErrorCode());
    }

    void HandleResult(Result<Message>&& result) {
      if (StopRequested()) {
        std::move(p_).SetError(Cancelled());
      } else if (NeedRetry(result)) {
        Retry();
      } else {
        std::move(p_).Set(std::move(result));
      }
    }

    void Retry() {
      AfterDelay().Subscribe([self = shared_from_this()](Result<void>&&) {
        self->Start();
      });
    }

    Future<Message> Call() {
      return fair_loss_->Call(pack_.method, pack_.request, pack_.options)
          .Via(runtime_->Executor());
    }

    Future<void> AfterDelay() {
      return runtime_->Timers()->After(backoff_()).Via(runtime_->Executor());
    }

   private:
    IChannelPtr fair_loss_;
    Backoff backoff_;
    IRuntime* runtime_;
    ParameterPack pack_;
    Promise<Message> p_;
  };

 public:
  ReliableChannelOnCallbacks(IChannelPtr fair_loss,
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
    std::make_shared<Retrier>(fair_loss_, backoff_params_, runtime_, pack,
                              std::move(p))
        ->Start();
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

    void Start() {
      auto result = Await(Call());
      while (NeedRetry(result)) {
        Await(AfterDelay()).ExpectOk();
        result = Await(Call());
      }
      HandleResult(std::move(result));
    }

   private:
    bool StopRequested() {
      return pack_.options.stop_advice.StopRequested();
    }

    bool NeedRetry(const Result<Message>& result) {
      return result.HasError() && IsRetriableError(result.GetErrorCode()) &&
             !StopRequested();
    }

    void HandleResult(Result<Message>&& result) {
      if (StopRequested()) {
        std::move(p_).SetError(Cancelled());
      } else {
        std::move(p_).Set(std::move(result));
      }
    }

    Future<Message> Call() {
      return fair_loss_->Call(pack_.method, pack_.request, pack_.options)
          .Via(runtime_->Executor());
    }

    Future<void> AfterDelay() {
      return runtime_->Timers()->After(backoff_()).Via(runtime_->Executor());
    }

   private:
    IChannelPtr fair_loss_;
    Backoff backoff_;
    IRuntime* runtime_;
    ParameterPack pack_;
    Promise<Message> p_;
  };

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

    ParameterPack pack = {method, request, options};
    await::fibers::Start(
        runtime_->Executor(),
        [retrier = Retrier(fair_loss_, backoff_params_, runtime_, pack,
                           std::move(p))]() mutable {
          retrier.Start();
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

// using ReliableChannel = ReliableChannelOnCallbacks;
using ReliableChannel = ReliableChannelOnFibers;

IChannelPtr MakeReliableChannel(IChannelPtr fair_loss,
                                Backoff::Params backoff_params,
                                IRuntime* runtime) {
  return std::make_shared<ReliableChannel>(std::move(fair_loss), backoff_params,
                                           runtime);
}

}  // namespace rpc