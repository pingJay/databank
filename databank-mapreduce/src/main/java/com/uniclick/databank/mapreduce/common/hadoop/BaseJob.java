package com.uniclick.databank.mapreduce.common.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.uniclick.databank.mapreduce.common.Constants;


/**
 * Base Job
 * 
 * @author kevin wang
 */
public abstract class BaseJob {

	/**
	 * get Conf
	 * 
	 * @param queue
	 * @return
	 */
	protected static Configuration getConf(String queue) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", HDFSUtil.getFSDefaultNameStr());
		conf.set("mapred.job.tracker", HDFSUtil.getMRJobTrackerStr());
		conf.set("mapred.job.queue.name", queue);
		conf.set("mapred.jar", Constants.SYS_PATH_RUN_JOB_JAR);
		//conf.set("mapred.jar","file:///D:\\workspace\\databank\\databank-mapreduce\\target/databank-mapreduce-2.0.0.jar");
		conf.setInt("mapred.reduce.tasks", Constants.JOB_CONF_REDUCER_NUMBER);
		return conf;
	}

	/**
	 * get Job
	 * 
	 * @param conf
	 * @param jobName
	 * @param jarCls
	 * @param mapperCls
	 * @param combinerCls
	 * @param reducerCls
	 * @param mapOutputKeyCls
	 * @param mapOutputValueCls
	 * @param outputKeyCls
	 * @param outputValueCls
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static Job getJob(Configuration conf, String jobName, Class jarCls, Class mapperCls, Class combinerCls, Class reducerCls, Class mapOutputKeyCls, Class mapOutputValueCls, Class outputKeyCls, Class outputValueCls) throws IOException {
		Job job = new Job(conf);
		if (jobName != null && "".equals(jobName)) {
			job.setJobName(jobName);
		}
		if (jarCls != null) {
			job.setJarByClass(jarCls);
		}
		if (mapperCls != null) {
			job.setMapperClass(mapperCls);
		}
		if (combinerCls != null) {
			job.setCombinerClass(combinerCls);
		}
		if (reducerCls != null) {
			job.setReducerClass(reducerCls);
		}
		if (mapOutputKeyCls != null) {
			job.setMapOutputKeyClass(mapOutputKeyCls);
		}
		if (mapOutputValueCls != null) {
			job.setMapOutputValueClass(mapOutputValueCls);
		}
		if (outputKeyCls != null) {
			job.setOutputKeyClass(outputKeyCls);
		}
		if (outputValueCls != null) {
			job.setOutputValueClass(outputValueCls);
		}
		return job;
	}

	/**
	 * set Compress Sequence File
	 * 
	 * @param outputPath
	 * @param job
	 */
	protected static void setCompressSequenceFile(String outputPath, Job job) {
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
	}

}
