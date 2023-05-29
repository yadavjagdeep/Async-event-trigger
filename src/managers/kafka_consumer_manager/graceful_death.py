import signal


class GracefulDeath:

    def __init__(self):
        self.received_signal = False
        self.received_term_signal = False
        self.last_signal = None

        catch_signals = [
            signal.SIGHUP,  # 1,
            signal.SIGINT,  # 2,
            signal.SIGQUIT,  # 3,
            signal.SIGUSR1,  # 10,
            signal.SIGUSR2,  # 12
            signal.SIGTERM,  # 15
        ]
        for signal_num in catch_signals:
            signal.signal(signal_num, self.handler)

    def handler(self, signal_num, frame):
        self.last_signal = signal
        if signal_num in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM, signal.SIGKILL]:
            print("In handler")
            self.received_term_signal = True
            print(self.received_term_signal)




