using System;
using Microsoft.Extensions.Logging;

namespace NRaft {
    public static class LoggerFactory {
        public static ILogger GetLogger<T>() {
            return new ConsoleLogger();
        }
    }

    public class DisposableAction : IDisposable
    {
        private Action action;

        public DisposableAction(Action action=null) {
            this.action = action;
        }

        public void Dispose()
        {
            if(action != null)
                action();
        }
    }

    public class ConsoleLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
        {
            return new DisposableAction();
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            Console.WriteLine(formatter(state, exception));
        }
    }
}