import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class HandlerImpl implements Handler {
    private final Duration timeout;
    private final Client client;

    public HandlerImpl(Client client, Duration timeout) {
        this.timeout = timeout;
        this.client = client;
    }

    @Override
    public Duration timeout() {
        return timeout;
    }

    @Override
    public void performOperation() {
        record ResultAddress(Result result, Address address) {}
        var delayedExecutor = CompletableFuture.delayedExecutor(timeout().get(ChronoUnit.MILLIS), TimeUnit.MILLISECONDS);
        var event = client.readData();
        event.recipients().stream()
                        .map(dst -> CompletableFuture.supplyAsync(() -> new ResultAddress(client.sendData(dst, event.payload()), dst)))
                .map(CompletableFuture::join)
                .filter(resArdd -> resArdd.result == Result.REJECTED)
                .forEach(resArdd -> CompletableFuture.supplyAsync(() -> client.sendData(resArdd.address, event.payload()), delayedExecutor));
    }
}
