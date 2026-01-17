package database.concurrency;

@SuppressWarnings("serial")
public class NoLockHeldException extends RuntimeException {
    NoLockHeldException(String message) {
        super(message);
    }
}

