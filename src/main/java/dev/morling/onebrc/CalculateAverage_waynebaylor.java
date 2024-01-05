/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class CalculateAverage_waynebaylor {

	static record Measurement(String location, Double value, boolean isComplete) {}

	static record Result(String location, Double min, Double max, Double average) {
		@Override
		public String toString() {
			return Math.round(min*10.0)/10.0 + "/" + Math.round(average*10.0)/10.0 + "/" + Math.round(max*10.0)/10.0;
		}
	}

	static class MeasurementTask implements Callable<Result> {
		private final String location;
		private final BlockingQueue<Measurement> queue;

		public MeasurementTask(String location, BlockingQueue<Measurement> queue) {
			this.location = location;
			this.queue = queue;
		}

		@Override
		public Result call() throws Exception {
			Double min = null;
			Double max = null;
			Double sum = 0.0;
			Integer count = 0;

			while(true) {
				Measurement m = this.queue.take();
				if(m.isComplete()) {
					break;
				}

				if(min == null || m.value() < min) {
					min = m.value();
				}
				if(max == null || m.value() > max) {
					max = m.value();
				}

				sum += m.value();
				count += 1;
			}

			return new Result(this.location, min, max, sum/count);
		}
	}

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        ExecutorService es = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<Result>> futures = new LinkedList<>();
        Map<String, BlockingQueue<Measurement>> locationToQueue = new ConcurrentHashMap<>();

        try (var br = new BufferedReader(new FileReader(new File(FILE)))) {
            String line;
            while ((line = br.readLine()) != null) {
            	int index = line.indexOf(";");
            	String location = line.substring(0, index);
            	Double value = Double.valueOf(line.substring(index + 1));

            	var queue = locationToQueue.get(location);
            	if(queue == null) {
            		queue = new LinkedBlockingQueue<Measurement>();
            		locationToQueue.put(location, queue);

            		Future<Result> future = es.submit(new MeasurementTask(location, queue));
            		futures.add(future);
            	}

            	queue.put(new Measurement(location, value, false));
            }

            // sentinel measurements
            for(var queue : locationToQueue.values()) {
            	queue.put(new Measurement(null, null, true));
            }

            // gather results
            Map<String, Result> results = new TreeMap<>();
            for(Future<Result> future : futures) {
            	Result r = future.get();
            	results.put(r.location(), r);
            }

            System.out.println(results.toString());
        }
        catch (Exception ex) {
            System.out.println("Error" + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
