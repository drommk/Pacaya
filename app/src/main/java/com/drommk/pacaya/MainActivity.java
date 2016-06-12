package com.drommk.pacaya;

import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscription;
import rx.android.schedulers.AndroidSchedulers;

public class MainActivity extends AppCompatActivity {

    public static final int PERIODIC_FLUSH_INTERVAL = 10;
    public static final int AUTOMATIC_FLUSH_SIZE = 100;
    public static final int BATCH_MAX_ITEMS = 50;
    public static final int MAX_ITEMS_IN_STORAGE = 1000;

    public final SampleStorageService sampleStorageService = new SampleStorageService();
    public final SampleFlushService sampleFlushService = new SampleFlushService();

    public final Pacaya<MyEvent> pacaya = new Pacaya.Builder<MyEvent>()
            .localStorageService(sampleStorageService)
            .flushService(sampleFlushService)
            .autoFlushSize(AUTOMATIC_FLUSH_SIZE)
            .batchMaxItems(BATCH_MAX_ITEMS)
            .maxItemsInStorage(MAX_ITEMS_IN_STORAGE)
            .build();

    private Random random = new Random();
    private TextView storageTextView;
    private Observable<Long> intervalObservable =
            Observable.interval(100, TimeUnit.MILLISECONDS)
                    .map(aLong -> (long) random.nextInt(1000000))
                    .filter(aLong -> aLong % 10 == 0)
                    .doOnNext(aLong1 -> pacaya.log(new MyEvent(Integer.parseInt(aLong1.toString()))));
    private Subscription intervalSubscription;
    private Subscription refresherSubscription;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        FloatingActionButton fab = (FloatingActionButton) findViewById(R.id.fab);
        storageTextView = (TextView) findViewById(R.id.storage);

        if (fab == null) {
            return;
        }

        fab.setOnClickListener(view -> {

            if (pacaya.isPeriodicFlushStarted()) {
                pacaya.stopPeriodicFlush();
                Snackbar.make(view, "Stopped periodic flush", Snackbar.LENGTH_SHORT).show();
                return;
            }
            Snackbar.make(view, "Started periodic flush", Snackbar.LENGTH_SHORT).show();
            pacaya.startPeriodicFlush(PERIODIC_FLUSH_INTERVAL, TimeUnit.SECONDS);
        });
    }

    @Override
    protected void onPause() {
        super.onPause();
        if (intervalSubscription != null && !intervalSubscription.isUnsubscribed()) {
            intervalSubscription.unsubscribe();
        }

        if (refresherSubscription != null && !refresherSubscription.isUnsubscribed()) {
            refresherSubscription.unsubscribe();
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (intervalSubscription == null || intervalSubscription.isUnsubscribed()) {
            intervalSubscription = intervalObservable.subscribe();
        }

        refresherSubscription = Observable.interval(100, TimeUnit.MILLISECONDS)
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(aLong -> storageTextView.setText(
                        String.format("FLUSHED(%d) | PRUNED(%d)%n%nSTORED(%d) : %s %n",
                                sampleFlushService.flushed,
                                sampleStorageService.pruned,
                                sampleStorageService.events.size(),
                                sampleStorageService.events.toString())));

    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        storageTextView = null;
    }

    public static class MyEvent {
        final Integer id;

        public MyEvent(Integer id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "MyEvent{" +
                    "id=" + id +
                    '}';
        }
    }

    public class SampleFlushService implements FlushService<MyEvent> {
        int flushed = 0;

        @Override
        public Observable<String> sendBatch(List<MyEvent> events) {
            flushed += events.size();
            return Observable.just(events.get(events.size() - 1).id.toString());
        }
    }

    public class SampleStorageService implements LocalStorageService<MyEvent> {
        List<MyEvent> events = Collections.synchronizedList(new ArrayList<>());
        int pruned = 0;

        @Override
        public Observable<List<MyEvent>> list(int limit) {
            int realLimit = Math.min(limit, events.size());
            return Observable.just(events.subList(0, realLimit));
        }

        @Override
        public Observable<Integer> count() {
            return Observable.just(events.size());
        }

        @Override
        public Observable<MyEvent> insert(List<MyEvent> items) {
            events.addAll(items);
            return Observable.just(items.get(items.size() - 1));
        }

        @Override
        public Observable<Integer> deleteUpTo(String eventId) {
            List<MyEvent> toDeleteEvents = new ArrayList<>();
            for (MyEvent event : events) {
                if (event.id < Integer.parseInt(eventId)) {
                    toDeleteEvents.add(event);
                }
            }
            events.removeAll(toDeleteEvents);

            return Observable.just(Integer.parseInt(eventId));
        }

        @Override
        public Observable<Integer> deleteOlderItems(Integer count) {
            List<MyEvent> obsoleteEvents = events.subList(0, count);
            pruned += obsoleteEvents.size();
            events.removeAll(obsoleteEvents);
            return Observable.just(obsoleteEvents.size());
        }
    }

}
