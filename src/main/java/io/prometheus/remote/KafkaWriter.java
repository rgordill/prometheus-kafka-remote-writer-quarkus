package io.prometheus.remote;

import java.io.IOException;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xerial.snappy.Snappy;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Stream;
import prometheus.Remote.WriteRequest;

@Path("/write")
public class KafkaWriter {

	static Logger logger = LoggerFactory.getLogger(KafkaWriter.class);
	
	@Inject
	@Stream("prometheus") // Emit on the channel 'metrics'
	Emitter<String> metrics;
	
    @POST
    public void write( byte[] byteArray) throws InvalidProtocolBufferException, IOException {
    	
		WriteRequest wr = WriteRequest.parseFrom(Snappy.uncompress(byteArray));

		metrics.send(JsonFormat.printer().print(wr));
    }
}