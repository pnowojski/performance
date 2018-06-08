package com.github.projectflink.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Throughput {

	private static final Logger LOG = LoggerFactory.getLogger(Throughput.class);

	static final int DEFAULT_PAYLOAD_SIZE = 12;
	static final int DEFAULT_DELAY = 0;
	static final int DEFAULT_LATENCY_FREQUENCY = 1_000_000;
	static final int DEFAULT_SLEEP_FREQUENCY = 0;
	static final int DEFAULT_LOG_FREQUENCY = 1000;
	enum ID_MODE { INCREASING, RANDOM };

	public static class Type extends Tuple4<Long, // sequence number from generator instance
			Integer,  // host ID on GCE
			Long, // timestamp
			byte[] // some payload
			> {

		private static final long serialVersionUID = -1625207099642006860L;

		public Type(Long value0, Integer value1, Long value2, byte[] value3) {
			super(value0, value1, value2, value3);
		}

		public Type() {
		}

		public Type copy() {
			return new Type(this.f0, this.f1, this.f2, this.f3);
		}
	}

	public static int convertHostnameToInt(String host) {
		return host.hashCode();
	}

	private static ID_MODE getIdMode(ParameterTool pt) {
		if (pt.has("idMode") && "random".equals(pt.get("idMode"))) {
			return ID_MODE.RANDOM;
		}
		// default
		return ID_MODE.INCREASING;
	}

	public static class Source extends RichParallelSourceFunction<Type> implements ListCheckpointed<Long> {
		private static final long serialVersionUID = -9116320247501922636L;

		final ParameterTool pt;
		byte[] payload;
		long id = 0;
		boolean running = true;
		long time = 0;

		public Source(ParameterTool pt) {
			this.pt = pt;
			payload = new byte[pt.getInt("payload", DEFAULT_PAYLOAD_SIZE)];
		}

		@Override
		public void run(SourceContext<Type> sourceContext) throws Exception {
			int delay = pt.getInt("delay", DEFAULT_DELAY);
			int latFreq = pt.getInt("latencyFreq", DEFAULT_LATENCY_FREQUENCY);
			int nextlat = 1000;
			int sleepFreq = pt.getInt("sleepFreq", DEFAULT_SLEEP_FREQUENCY);
		//	String host = InetAddress.getLocalHost().getHostName();
			int host = convertHostnameToInt(InetAddress.getLocalHost().getHostName());
			final ID_MODE idMode = getIdMode(pt);

			while(running) {
				if(delay > 0) {
					if(id % sleepFreq == 0) {
						try { Thread.sleep(delay); } catch (InterruptedException e) { e.printStackTrace();}
					}
				}
				// move the ID for the latency so that we distribute it among the machines.
				if(id % latFreq == nextlat) {
					time = System.currentTimeMillis();
					if(--nextlat <= 0) {
						nextlat = 1000;
					}
				}
				++id;


				if (idMode == ID_MODE.RANDOM) {
					sourceContext.collectBatch(new Type(ThreadLocalRandom.current().nextLong(), host, time, payload));
				} else {
					sourceContext.collectBatch(new Type(id, host, time, payload));
				}

				time = 0;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public List<Long> snapshotState(long l, long l1) throws Exception {
			return Collections.singletonList(id);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			this.id = state.isEmpty() ? 0 : state.get(0);
		}
	}

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);
		see.setRestartStrategy(RestartStrategies.noRestart());

		if (pt.has("timeout")) {
			see.setBufferTimeout(pt.getLong("timeout"));
		}

		if (pt.has("ft")) {
			long checkpointInterval = pt.getLong("ft");
			if (checkpointInterval > 0) {
				see.enableCheckpointing(checkpointInterval);
			}
		}

		if (pt.has("maxParallelism")) {
			see.setMaxParallelism(pt.getInt("maxParallelism"));
		}

		DataStream<Type> source = see.addSource(new Source(pt) );

		DataStream<Type> repartitioned = source.keyBy(0);
		for(int i = 0; i < pt.getInt("repartitions", 1) - 1;i++) {
			repartitioned = repartitioned.map(new MapFunction<Type, Type>() {
				@Override
				public Type map(Type in) throws Exception {
					Type out = in.copy();
					out.f0++;
					return out;
				}
			}).keyBy(0);
		}
		repartitioned.flatMap(new ThroughputMeasuringFlatMap(
				8 + 8 + 4 + pt.getInt("payload", DEFAULT_PAYLOAD_SIZE),
				pt.getInt("logfreq", DEFAULT_LOG_FREQUENCY)));
		//System.out.println("plan = "+see.getExecutionPlan());;
		see.execute("Flink Throughput Job with: "+pt.toMap());
	}

}
