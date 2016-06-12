package com.drommk.pacaya;

import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import rx.Observable;

public class MainActivity extends AppCompatActivity {


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        FloatingActionButton fab = (FloatingActionButton) findViewById(R.id.fab);

        if (fab == null) {
            return;
        }

        fab.setOnClickListener(new View.OnClickListener() {
            boolean started = false;
            Pacaya<MyEvent> pacaya;
            Random rand = new Random();

            @Override
            public void onClick(View view) {
                if (started) {
                    pacaya.stopPeriodicFlush();
                    started = false;
                    return;
                }
                pacaya = new Pacaya.Builder<MyEvent>()
                        .autoFlushSize(1)
                        .batchMaxItems(1)
                        .batchMaxUncompressedSize(1)
                        .maxItemsInStorage(1)
                        .localStorageService(new LocalStorageService<MyEvent>() {
                            @Override
                            public Observable<List<MyEvent>> list(int limit) {
                                return null;
                            }

                            @Override
                            public Observable<Integer> count() {
                                return null;
                            }

                            @Override
                            public Observable<MyEvent> insert(List<MyEvent> items) {
                                return null;
                            }

                            @Override
                            public Observable<Integer> deleteUpTo(String eventId) {
                                return null;
                            }

                            @Override
                            public Observable<Integer> deleteOlderItems(Integer count) {
                                return null;
                            }
                        })
                        .flushService(events -> {
                            System.out.printf("Flushed %d events%n", events.size());
                            return Observable.just(events.get(events.size() - 1).ts.toString());
                        })
                        .build();

                pacaya.startPeriodicFlush(1, TimeUnit.SECONDS);
                Observable.interval(rand.nextInt(), TimeUnit.MILLISECONDS).doOnNext(System.out::println);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    public static class MyEvent {
        final Long ts;

        public MyEvent(Long ts) {
            this.ts = ts;
        }
    }

}
