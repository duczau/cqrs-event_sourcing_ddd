package com.techbank.cqrs.core.infrastructure;

import java.util.List;

import com.techbank.cqrs.core.events.BaseEvent;

public interface EventStore {
    void saveEvents(String aggregateId, Iterable<BaseEvent> events, int expectedVersion);
    List<BaseEvent> getEvents(String aggregateId);
}
