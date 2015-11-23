package org.van.providers.impl;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import org.van.providers.ItemStoreProvider;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Created by vly on 11/15/2015.
 */
public class SimpleDbStoreProvider extends ItemStoreProvider {

    private final AmazonSimpleDB sdbClient;
    private final String domain;

    public SimpleDbStoreProvider(Optional<AmazonSimpleDB> simpleDb, final String domain) {
        Objects.requireNonNull(simpleDb);
        sdbClient = simpleDb.orElseGet(() -> new AmazonSimpleDBClient().withRegion(Regions.US_WEST_2));
        this.domain = domain;
    }

    @Override
    public String toString() {
        return String.format("SimpleDB (domain:\"%s\")", domain);
    }

    @Override
    public SimpleDbStoreProvider initialize() {
        CreateDomainRequest request = new CreateDomainRequest(domain);
        sdbClient.createDomain(request);
        return this;
    }

    @Override
    public void storeItem(Item item) {
        PutAttributesRequest request = new PutAttributesRequest()
            .withDomainName(domain)
            .withItemName(item.getName())
            .withAttributes(composeReplaceableAttribs(item.getAttributes()))
            ;
        sdbClient.putAttributes(request);
    }

    private List<ReplaceableAttribute> composeReplaceableAttribs(List<Attribute> attributes) {
        Objects.requireNonNull(attributes);
        List<ReplaceableAttribute> replaceables = new LinkedList<>();
        attributes.forEach(attribute ->
            replaceables.add(new ReplaceableAttribute(attribute.getName(), attribute.getValue(), true)));
        return replaceables;
    }
}
