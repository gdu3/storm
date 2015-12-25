/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.utils;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.metric.api.IStatefulObject;
import java.util.concurrent.ArrayBlockingQueue;
import backtype.storm.utils.Utils;


/**
 *
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is
 * the ability to catch up to the producer by processing tuples in batches.
 */
public class DisruptorQueue implements IStatefulObject {
    
    static final Object INTERRUPT = new Object();
    // TODO: consider having a threadlocal cache of this variable to speed up reads?
    volatile boolean consumerStartedFlag = false;
    
    private static String PREFIX = "disruptor-";
    private String _queueName = "";

    private long _waitTimeout;
    private final ArrayBlockingQueue _buffer;
    
    public DisruptorQueue(String queueName, int size, long timeout) {
        this._queueName = PREFIX + queueName;
        _buffer = new ArrayBlockingQueue(size, true);
        _waitTimeout = timeout;
    }
    
    public String getName() {
      return _queueName;
    }
    
    public void consumeBatch(EventHandler<Object> handler) {
        consumeBatchToCursor(_buffer.size(), handler);
    }
    
    public void haltWithInterrupt() {
        publish(INTERRUPT);
    }
    
    public void consumeBatchWhenAvailable(EventHandler<Object> handler) {
        int size = _buffer.size();
        if(size > 0) {
            consumeBatchToCursor(size, handler);
        } else {
            consumeBatchWithWaitingTime(_waitTimeout, handler);
        }
    }

    private void consumeBatchWithWaitingTime(long timeout, EventHandler<Object> handler) {
        try {
            Object o = _buffer.poll(timeout, TimeUnit.MILLISECONDS);
            if(o==null) return;
            else if (o==INTERRUPT) {
                throw new InterruptedException("Disruptor processing interrupted");
            } else {
                int size = _buffer.size();
                handler.onEvent(o, 0, size==0);
                if(size!=0) {
                    consumeBatchToCursor(size, handler);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private void consumeBatchToCursor(int size, EventHandler<Object> handler) {
        for(int i=1; i<=size; i++) {
            try {
                Object o = _buffer.poll();
                if(o==null) continue;
                else if (o==INTERRUPT) {
                    throw new InterruptedException("Disruptor processing interrupted");
                } else {
                    handler.onEvent(o, i, i==size);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    /*
     * Caches until consumerStarted is called, upon which the cache is flushed to the consumer
     */
    public void publish(Object obj) {
        try {
            _buffer.put(obj);
        } catch (InterruptedException e) {
            throw new RuntimeException("This code should be unreachable! InterruptedException");
        } catch (NullPointerException e) {
            throw new RuntimeException("This code should be unreachable! NullPointerException");
        }
    }
    
   
    
    public void consumerStarted() {
    }
    
    public long  population() { return 1000; }
    public long  capacity()   { return 1000; }
    public long  writePos()   { return 1000; }
    public long  readPos()    { return 1000; }
    public float pctFull()    { return 0; }

    @Override
    public Object getState() {
        Map state = new HashMap<String, Object>();
        // get readPos then writePos so it's never an under-estimate
        long rp = 1000;
        long wp = 1000;
        state.put("capacity",   capacity());
        state.put("population", wp - rp);
        state.put("write_pos",  wp);
        state.put("read_pos",   rp);
        return state;
    }

    public static class ObjectEventFactory implements EventFactory<MutableObject> {
        @Override
        public MutableObject newInstance() {
            return new MutableObject();
        }        
    }
}
