package org.van.providers.impl;

import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;

import org.apache.commons.io.IOUtils;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.van.Accumulator;
import org.van.providers.ItemSourceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Created by vly on 11/15/2015.
 */
public class CsvSourceProvider extends ItemSourceProvider {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final InputStream inputStream;
    private boolean streamOwnership = false;
    private CsvListReader reader;

    Item prevItem = null;
    AtomicReference<Item> currentItem = new AtomicReference<>();
    Accumulator<Void,Item,List<String>,String> accumulator;

    public CsvSourceProvider(InputStream inputStream) {
        Objects.requireNonNull(inputStream);
        this.inputStream = inputStream;
    }

    /**
     * Sets the stream ownership of this instance. If true, then this instance will close the
     * input stream it is instantiated with. If false, then it will not.
     *
     * @param streamOwnership true to delegate stream ownership to this instance
     *
     * @return this instnace
     */
    public CsvSourceProvider withStreamOwnership(boolean streamOwnership) {
        this.streamOwnership = streamOwnership;
        return this;
    }

    @Override
    public String toString() {
        return String.format("CSV file stream %s", inputStream);
    }

    @Override
    public ItemSourceProvider initialize() {
        reader = new CsvListReader(new InputStreamReader(inputStream, UTF8), CsvPreference.EXCEL_PREFERENCE);
        accumulator = new Accumulator<>(
            row -> row.get(0),
            Item::getName,
            row -> new Item().withName(row.get(0)),
            currentItem::set
        );
        accumulator.withChained(
            new Accumulator<Item, Attribute, List<String>, String>(
                row -> row.get(1) + row.get(2),
                attribute -> attribute.getName() + attribute.getValue(),
                row -> new Attribute(row.get(1), row.get(2)),
                attribute -> {}
            ) {
                protected Optional<Attribute> transition(Optional<Item> item, List<String> row) {
                    Optional<Attribute> ra = super.transition(item, row);
                    if (ra.isPresent() && item.isPresent()) {
                        List<Attribute> attribs = item.get().getAttributes();
                        attribs.add(ra.get());
                        item.get().withAttributes(attribs);
                    }
                    return ra;
                }
            }
        );
        return this;
    }

    @Override
    public void iterateItems(Consumer<Item> consumer) {
        try {
            reader.read();  // the headers
            Item item = iterateUntilNew(accumulator, reader, currentItem.get());
            while (null != item) {
                if (null != prevItem) {
                    consumer.accept(prevItem);
                }
                prevItem = item;
                item = iterateUntilNew(accumulator, reader, currentItem.get());
            }
            if (null != prevItem) {
                consumer.accept(prevItem);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Error while iterating items", t);
        }
    }

    Item iterateUntilNew(Accumulator<Void,Item,List<String>,String> acc, CsvListReader reader,
                         Item currentRef) throws IOException {
        Item item = null;
        List<String> row = reader.read();
        while (null != row) {
            acc.accumulate(row);
            item = currentItem.get();
            if ((null == currentRef) || !item.getName().equals(currentRef.getName())) {
                break;
            }
            row = reader.read();
        }
        return item;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
        if (streamOwnership) {
            IOUtils.closeQuietly(inputStream);
        }
    }
}
