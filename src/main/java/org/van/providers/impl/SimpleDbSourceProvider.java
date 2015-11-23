package org.van.providers.impl;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import org.apache.log4j.Logger;
import org.van.RetryUtility;
import org.van.providers.ItemSourceProvider;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Created by vly on 11/15/2015.
 */
public class SimpleDbSourceProvider extends ItemSourceProvider {

    public static final int MAXRETRIES = 3;

    private static final Logger logger =
        Logger.getLogger(SimpleDbSourceProvider.class);

    private final AmazonSimpleDB sdbClient;
    private final String query;
    private final Optional<String> startToken;

    public SimpleDbSourceProvider(Optional<AmazonSimpleDB> simpleDb, String query,
                                  Optional<String> startToken) {
        Objects.requireNonNull(simpleDb);
        Objects.requireNonNull(query);
        Objects.requireNonNull(simpleDb);
        sdbClient = simpleDb.orElseGet(() -> new AmazonSimpleDBClient().withRegion(Regions.US_WEST_2));
        this.query = query;
        this.startToken = startToken;
    }

    @Override
    public String toString() {
        return String.format("SimpleDB (query:\"%s\")", query);
    }

    @Override
    public void iterateItems(Consumer<Item> consumer) {
        AtomicReference<String> tokenRef = new AtomicReference<>(startToken.orElse(null));
        do {
            logger.debug(String.format("Querying SimpleDB %s with next token %s",
                query, tokenRef.get()));

            tokenRef.set(RetryUtility.performWithRetry((nextToken, trial) -> {
                SelectRequest request = new SelectRequest(query, true);
                if (null != nextToken) {
                    request = request.withNextToken(nextToken);
                }
                SelectResult result = sdbClient.select(request);
                result.getItems().stream().forEach(consumer);
                return result.getNextToken();
            }, tokenRef.get(), MAXRETRIES));
        } while (null != tokenRef.get());
    }
}
