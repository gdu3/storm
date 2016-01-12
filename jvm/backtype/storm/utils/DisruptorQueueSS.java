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
import com.lmax.disruptor.util.Util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.metric.api.IStatefulObject;


/**
 *
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is
 * the ability to catch up to the producer by processing tuples in batches.
 */
public class DisruptorQueueSS implements IStatefulObject {
    
    RingBuffer<MutableObject> _buffer;
    SequenceBarrier _barrier;
    
    private static String PREFIX = "disruptor-";
    private String _queueName = "";
    private int t_id;
    private long _waitTimeout;
    private final Sequence[] _consumerSequences;
    private final Sequence _workSequence;
    
    public DisruptorQueueSS(String queueName, ClaimStrategy claim, WaitStrategy wait, long timeout, int consumers) {
         this._queueName = PREFIX + queueName;
        t_id = 0;
        _buffer = new RingBuffer<MutableObject>(new ObjectEventFactory(), claim, wait);
        _barrier = _buffer.newBarrier();
        _waitTimeout = timeout;
        _consumerSequences = new Sequence[consumers];
        final long cursor = _buffer.getCursor();
        _workSequence = new Sequence();
        _workSequence.set(cursor);
        for(int i=0; i<consumers; i++) {
            _consumerSequences[i] = new Sequence();
            _consumerSequences[i].set(cursor);
        }
        _buffer.setGatingSequences(_consumerSequences);
    }
    
    public String getName() {
      return _queueName;
    }
 
    // temporary empty
    public void haltWithInterrupt() {
    }
    
    public void consumeBatchWhenAvailable(EventHandler<Object> handler, Integer id) {
        boolean processedSequence = true;
        long nextSequence = 0;
        while (true)
        {
            try {
                if (processedSequence)
                {
                    processedSequence = false;
                    nextSequence = _workSequence.incrementAndGet();
                    _consumerSequences[id].set(nextSequence - 1L);
                }

                _barrier.waitFor(nextSequence);
                MutableObject mo = _buffer.get(nextSequence);
                Object o = mo.o;
                mo.setObject(null);
                handler.onEvent(o, 0, true);

                processedSequence = true;

            } catch (Exception e) {
                throw new RuntimeException(e);
            }    
        }
    }

    public void publish(Object obj) {
        try {
            publish(obj, true);
        } catch (InsufficientCapacityException ex) {
            throw new RuntimeException("This code should be unreachable!");
        }
    }
    
    public void publish(Object obj, boolean block) throws InsufficientCapacityException {
        publishDirect(obj, block);
    }
    
    private void publishDirect(Object obj, boolean block) throws InsufficientCapacityException {
        final long id;
        if(block) {
            id = _buffer.next();
        } else {
            id = _buffer.tryNext(1);
        }
        final MutableObject m = _buffer.get(id);
        m.setObject(obj);
        _buffer.publish(id);
    }
    
    public synchronized String consumerStarted() {
        return Integer.toString(t_id++);
    }
    
    public long  population() { return (writePos() - readPos()); }
    public long  capacity()   { return _buffer.getBufferSize(); }
    public long  writePos()   { return _buffer.getCursor(); }
    public long  readPos()    { return  Util.getMinimumSequence(_consumerSequences);}
    public float pctFull()    { return (1.0F * population() / capacity()); }

    @Override
    public Object getState() {
        Map state = new HashMap<String, Object>();
        // get readPos then writePos so it's never an under-estimate
        long rp = readPos();
        long wp = writePos();
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
