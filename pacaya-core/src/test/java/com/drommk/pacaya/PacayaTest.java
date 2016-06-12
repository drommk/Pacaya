package com.drommk.pacaya;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.observers.TestSubscriber;
import rx.plugins.DebugHook;
import rx.plugins.RxJavaPlugins;
import rx.plugins.SimpleDebugNotificationListener;
import rx.schedulers.TestScheduler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by ericpalle on 10/9/15.
 */
public class PacayaTest {

    private TestSubscriber<Event> testSubscriber;
    private TestScheduler testScheduler;

    public static class Event {

        String id;
        String content;

        public Event(String id, String content) {
            this.id = id;
            this.content = content;
        }

    }

    static {
        RxJavaPlugins.getInstance().registerObservableExecutionHook(new DebugHook<>(new SimpleDebugNotificationListener()));
    }

    @Mock
    LocalStorageService<Event> localStorageServiceServiceMock;

    @Mock
    FlushService<Event> flushServiceMock;

    @Mock
    ReportService<Event> reportServiceMock;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        reset(localStorageServiceServiceMock, flushServiceMock, reportServiceMock);
        testSubscriber = new TestSubscriber<>();
        testScheduler = new TestScheduler();
    }

    @Test
    public void testLogOneEventWhichTriggersAutoPruneAndAutoFlush() throws IOException {
        //PREPARE
        final int maxItemsInStorage = 400;
        final int batchMaxItems = 1000;
        final Event testEvent = new Event("1abcd", "test");
        final Event testReportEvent = new Event("loss_report", "1");
        final int itemsInStorage = 500;
        int itemsToPrune = 140;

        final List<Event> initialBatch = Collections.singletonList(testEvent);
        final List<Event> reportBatch = Collections.singletonList(testReportEvent);

        when(localStorageServiceServiceMock.insert(initialBatch)).thenReturn(Observable.just(testEvent));
        when(localStorageServiceServiceMock.count()).thenReturn(Observable.just(itemsInStorage));
        when(localStorageServiceServiceMock.list(anyInt())).thenReturn(Observable.just(initialBatch));
        when(localStorageServiceServiceMock.deleteOlderItems(anyInt())).thenReturn(Observable.just(itemsToPrune));
        when(localStorageServiceServiceMock.list(batchMaxItems)).thenReturn(Observable.just(initialBatch));
        when(flushServiceMock.sendBatch(initialBatch)).thenReturn(Observable.just(testEvent.id));
        when(localStorageServiceServiceMock.deleteUpTo(testEvent.id)).thenReturn(Observable.just(1));

        when(reportServiceMock.createLossReport(any(), eq(ReportService.BUFFER_FULL))).thenReturn(testReportEvent);

        when(localStorageServiceServiceMock.insert(any())).thenReturn(Observable.just(testReportEvent));
        when(reportServiceMock.getMaxIdFromBatch(anyObject())).thenReturn("maxbatchId");

        Pacaya<Event> pacaya = new Pacaya.Builder<Event>()
                .localStorageService(localStorageServiceServiceMock)
                .flushService(flushServiceMock)
                .reportService(reportServiceMock)
                .autoFlushSize(1L)
                .batchMaxItems(batchMaxItems)
                .maxItemsInStorage(maxItemsInStorage)
                .timeScheduler(testScheduler)
                .build();

        //EXECUTE
        pacaya.log(testEvent);
        testScheduler.advanceTimeBy(70, TimeUnit.SECONDS);

        //ASSERT
        verify(localStorageServiceServiceMock, times(1)).insert(initialBatch);
        verify(localStorageServiceServiceMock, times(1)).count();
        verify(localStorageServiceServiceMock, times(1)).deleteOlderItems(itemsToPrune);
        verify(localStorageServiceServiceMock, times(1)).list(itemsToPrune);
        verify(localStorageServiceServiceMock, times(1)).list(batchMaxItems);
        verify(flushServiceMock, times(1)).sendBatch(initialBatch);
        verify(localStorageServiceServiceMock, times(1)).deleteUpTo("1abcd");
        verify(localStorageServiceServiceMock, times(1)).insert(reportBatch);
        verifyNoMoreInteractions(localStorageServiceServiceMock, flushServiceMock);
    }

    @Test
    public void testPeriodicFlushWorksAsExpected() {
        //PREPARE
        final List<Event> itemsAtStart = generateEvents(0, 5);
        final List<Event> itemsAfterFirstFlush = generateEvents(5, 10);

        when(localStorageServiceServiceMock.list(anyInt())).thenAnswer(new CountAnswer<Observable<List<Event>>>() {
            @Override
            public Observable<List<Event>> call(int count) {
                return count == 1 ? Observable.just(itemsAtStart) : Observable.just(itemsAfterFirstFlush);
            }
        });

        when(localStorageServiceServiceMock.count()).thenAnswer(new CountAnswer<Observable<Integer>>() {
            @Override
            public Observable<Integer> call(int count) {
                return count == 1 ? Observable.just(itemsAtStart.size()) : Observable.just(itemsAfterFirstFlush.size());
            }
        });

        when(flushServiceMock.sendBatch(anyList())).thenReturn(Observable.just("lastReceivedId"));

        Pacaya<Event> pacaya = new Pacaya.Builder<Event>()
                .localStorageService(localStorageServiceServiceMock)
                .flushService(flushServiceMock)
                .timeScheduler(testScheduler)
                .subscriber(testSubscriber)
                .build();

        //EXECUTE
        pacaya.startPeriodicFlush(1, TimeUnit.HOURS);
        testScheduler.advanceTimeBy(1, TimeUnit.HOURS);
        testScheduler.advanceTimeBy(1, TimeUnit.HOURS);

        //ASSERT
        testSubscriber.assertNoErrors();
        verify(flushServiceMock, times(1)).sendBatch(itemsAtStart);
        verify(flushServiceMock, times(1)).sendBatch(itemsAfterFirstFlush);
    }

    private List<Event> generateEvents(int offset, int count) {
        List<Event> items = new ArrayList<>();
        for (long i = offset; i < count + offset; i++) {
            items.add(new Event(i + "abcd", "event" + i));
        }
        return items;
    }

    private abstract class CountAnswer<T> implements Answer<T> {
        int count = 0;

        @Override
        public T answer(InvocationOnMock invocation) throws Throwable {
            count++;
            return call(count);
        }

        public abstract T call(int count);
    }
}
