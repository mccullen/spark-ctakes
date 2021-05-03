/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package icapa.spark.functions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;

import icapa.spark.Util;
import icapa.spark.models.NotSerializable;
import icapa.spark.models.SerializableClass;
import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.ctakes.dictionary.lookup2.util.UmlsUserApprover;
import org.apache.spark.broadcast.Broadcast;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.util.XMLSerializer;
import org.apache.spark.api.java.function.Function;

/**
 * This is a task-level function. There can be multiple tasks per node
 */
public class CtakesFunction implements Function<String, String> {

	// Called once. Then readObject gets called nPartitions times to serialize to every node
	public CtakesFunction() {
	}

	public CtakesFunction(Broadcast<SerializableClass> var, NotSerializable notSerializable) {
	    _test = var;
	    _notSerializable = notSerializable;
	}

	private Broadcast<SerializableClass> _test;
	private NotSerializable _notSerializable;

    // Spark works by serializing everything w/i rdd functions like map.
	// UIMA components are not serializable, so we make them transient so spark doesn't
	// try to serialize them. We will initialize them in readObject(), which is used
	// under the hood during Spark's Serialization for classes (like this one) which are
	// Serializable. It will get called only once per partition/task. When a new batch comes in, it will
	// get called again for each partition/task.
	transient JCas jcas = null;
	transient AnalysisEngineDescription aed = null;
	transient AnalysisEngine aae = null;

	private void setup() throws Exception {
		// This will get called once per partition FOR EACH BATCH OF RDDs. So there is some overhead here since you
		// will be recreating the pipeline for each batch
		System.setProperty(UmlsUserApprover.KEY_PARAM, Util.getConfigProperty("umls.key"));
		System.setProperty("ctakes.umlspw", "");
		this.jcas = JCasFactory.createJCas();
		//this.aed = CTAKESClinicalPipelineFactory.getDefaultPipeline();
		//this.aed = AnalysisEngineFactory.createEngineDescription(Chunker.class);
		PiperFileReader piperFileReader = new PiperFileReader();
		piperFileReader.loadPipelineFile("C:/root/vdt/icapa/nlp/custom-components-repos/custom-components/reference/piper-files/default.piper");
        PipelineBuilder pipelineBuilder = piperFileReader.getBuilder();
        pipelineBuilder.set(UmlsUserApprover.KEY_PARAM, Util.getConfigProperty("umls.key"));
        pipelineBuilder.build();
        this.aed = pipelineBuilder.getAnalysisEngineDesc();
        this.aae = AnalysisEngineFactory.createEngine(this.aed);
	}
	
	private void readObject(ObjectInputStream in) throws Exception {
		try {
			in.defaultReadObject();
			this.setup();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (UIMAException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String call(String paragraph) throws Exception {
		//return test1(paragraph);
		//return test2(paragraph);
		//return test3(paragraph);
		//return test4(paragraph);
		return test5(paragraph);
	}

	private String test5(String paragraph) throws Exception {
		this.jcas.setDocumentText(paragraph);
		// Code from SimplePipeline.runPipeline(final CAS aCas, final AnalysisEngineDescription... aDescs)
		aae.process(this.jcas);
		// Signal end of processing
		//aae.collectionProcessComplete();
		// Destroy
		//LifeCycleUtil.destroy(aae);
		String result = getXmi(jcas);
		this.jcas.reset();
		return result;
	}

	private String getXmi(JCas jcas) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		XmiCasSerializer xmiSerializer = new XmiCasSerializer(jcas.getTypeSystem());
		XMLSerializer xmlSerializer = new XMLSerializer(baos, true);
		xmiSerializer.serialize(jcas.getCas(),xmlSerializer.getContentHandler());
		return baos.toString("utf-8");
	}

	private String test4(String paragraph) throws Exception {
		// This works, but is inefficient
		this.jcas.setDocumentText(paragraph);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimplePipeline.runPipeline(this.jcas, this.aed);
		XmiCasSerializer xmiSerializer = new XmiCasSerializer(jcas.getTypeSystem());
		XMLSerializer xmlSerializer = new XMLSerializer(baos, true);
		xmiSerializer.serialize(jcas.getCas(),xmlSerializer.getContentHandler());
		this.jcas.reset();
		return baos.toString("utf-8");
	}

	private String test2(String paragraph) throws Exception {
		this.jcas.setDocumentText(paragraph);

		// final AnalysisEngineDescription aed = getFastPipeline(); // Outputs
		// from default and fast pipelines are identical
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SimplePipeline.runPipeline(this.jcas, this.aed);
		XmiCasSerializer xmiSerializer = new XmiCasSerializer(jcas.getTypeSystem());
		XMLSerializer xmlSerializer = new XMLSerializer(baos, false);
		//XMLSerializer xmlSerializer = new XMLSerializer(baos, true);
		xmiSerializer.serialize(jcas.getCas(),xmlSerializer.getContentHandler());
		this.jcas.reset();
		return baos.toString("utf-8");
	}

	private String test1(String paragraph) throws Exception {

		PiperFileReader piperReader = new PiperFileReader();

		PipelineBuilder builder = piperReader.getBuilder();
		builder.set("umlsKey", Util.getConfigProperty("umls.key"));
		piperReader.loadPipelineFile("C:/root/vdt/icapa/nlp/custom-components-repos/custom-components/reference/piper-files/default.piper");
		builder.run(paragraph);

		XmiCasSerializer xmiSerializer = new XmiCasSerializer(jcas.getTypeSystem());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		XMLSerializer xmlSerializer = new XMLSerializer(baos, true);
		xmiSerializer.serialize(jcas.getCas(),xmlSerializer.getContentHandler());
		this.jcas.reset();
		String result = baos.toString("utf-8");
		String result2 = new String(baos.toByteArray(), Charset.defaultCharset());
		if (result.equals(result2)) {
			System.out.println("equal");
		} else {
			System.out.println("NOT EQUAL");
		}
		return result2;
	}

}
