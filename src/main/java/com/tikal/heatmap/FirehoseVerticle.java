package com.tikal.heatmap;

import java.util.Date;
import java.util.Random;

import io.vertx.core.AbstractVerticle;
import io.vertx.redis.RedisClient;

public class FirehoseVerticle extends AbstractVerticle {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FirehoseVerticle.class);

	private RedisClient redis;
	
	@Override
	public void start() throws Exception {
		redis = RedisClient.create(vertx, config());
		vertx.setPeriodic(config().getInteger("checkinsInterval"),(l) -> sendCheckin());	
	}

	private void sendCheckin() {
		final int i = new Random().nextInt(config().getInteger("maxAddressSeqValue",4700));
		redis.get("ADDSEQ-" + i , res -> {
			  if (res.succeeded()){
				  if(res.result()==null)
					  logger.error("Address is null - Will not send");
				  else
					  sendCheckin(new Date().getTime() + "@" +res.result());
			  }
			  else
				  logger.error("Fail to get on Redis",res.cause());
		});		
	}

	private void sendCheckin(final String checkin) {
		if(checkin!=null){
			logger.debug("Sending checkin:{}",checkin);
			vertx.createHttpClient()
				.post(	config().getInteger("http.server.port"),
						config().getString("http.server.address","localhost"),
						"/", 
						response -> logger.debug("Received response with status code " + response.statusCode()))
				.putHeader("content-type", "text/plain")
				.end(checkin);
		}
	}

}
