package com.example.starter;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.mqtt.MqttClient;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * 启动类
 *
 * @author dragon
 * @date 2022/01/30
 */
public class MainVerticle extends AbstractVerticle {
  private final Logger log = LoggerFactory.getLogger(MainVerticle.class);
  private final Random random = new Random();

  @Override
  public void start(Promise<Void> startPromise) {
    InfluxDbConfig influxDbConfig = new InfluxDbConfig("http://localhost:8086", "root", "root");

    JsonObject dbConfig = new JsonObject()
      .put("url", "jdbc:sqlite:test.db")
      .put("driver_class", "org.sqlite.JDBC")
      .put("max_pool_size", 16);

    JDBCPool pool = JDBCPool.pool(
      vertx,
      // configure the connection
      dbConfig
    );

    //建立MQ连接
    MqttClient client = MqttClient.create(vertx);
    client.connect(1883, "localhost", str -> {
      System.out.println("mqtt 1883 connected");
      //添加订阅
      client.publishHandler(s -> {
        System.out.println(s.payload().toString());
      }).subscribe("test", 2);
    });

    Router router = Router.router(vertx);
    router.get("/save").respond((routingContext) -> {
        //随机温度
        int temperature = random.nextInt(21) + 16;
        Data data = new Data();
        data.setSensorName("testSensor");
        data.setTemperature(temperature);
        HashMap<String, String> tagMap = new HashMap<>(1);
        tagMap.put("id", "1");
        HashMap<String, Object> filedMap = new HashMap<>(1);
        filedMap.put("temperature", temperature);
        return Future.fromCompletionStage(CompletableFuture.supplyAsync(() -> {
          //保存至Influx
          influxDbConfig.insert("test", "temperature", tagMap, filedMap);
          //发送至MQ
          client.publish("test", Buffer.buffer(String.valueOf(temperature)), MqttQoS.AT_LEAST_ONCE, false, false);
          pool.query("update temperature_data set temperature=" + temperature + " where id =1")
            .execute().onSuccess(rows -> {
            });
          return data;
        }));
      }
    );

    router.get("/queryResult").respond(
      routingContext -> Future.fromCompletionStage(CompletableFuture.supplyAsync(
        () -> {
          return influxDbConfig.query("test", "SELECT MEAN(temperature) FROM \"temperature\" WHERE time > now() - 20m");
        }
        )
      )
    );

    router.get("/testBlock").respond((routingContext) -> {
      log.info("testBlock in");
      Future<String> testBlockResult = Future.fromCompletionStage(CompletableFuture.supplyAsync(() -> {
        try {
          Thread.sleep(3000L);
          log.info("testBlock result");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return "ok";
      }));
      log.info("testBlock out");
      return testBlockResult;
    });

    vertx.createHttpServer().requestHandler(router).listen(8080, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8080");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }


  /**
   * 数据实体
   */
  private static class Data {
    /**
     * 温度
     */
    private int temperature;
    /**
     * 传感器名
     */
    private String sensorName;

    public int getTemperature() {
      return temperature;
    }

    public void setTemperature(int temperature) {
      this.temperature = temperature;
    }

    public String getSensorName() {
      return sensorName;
    }

    public void setSensorName(String sensorName) {
      this.sensorName = sensorName;
    }
  }
}
