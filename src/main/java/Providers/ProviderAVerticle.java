package Providers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class ProviderAVerticle extends AbstractVerticle{
    private static final Logger LOGGER = LogManager.getLogger(ProviderAVerticle.class);
    SimpleDateFormat date = new SimpleDateFormat("yyyyMMdd");
    SimpleDateFormat dateformat = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
    String dateStr = date.format(System.currentTimeMillis());
    String timestamp = dateformat.format(System.currentTimeMillis());

    protected RabbitMQClient client;
    protected Channel channel;
    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
    }

    @Override
    public void start(Future<Void> startFuture){
        String logFront ="[ "+config().getString("TestID")+"-"+ dateStr+"-"+config().getString("SeriesID")+" ] [ "
                +config().getString("host")+"/"+config().getString("AdapterName")+"/"+ProviderAVerticle.class.getName()+" ] [ "+timestamp+" ] ";
        //创建连接
        RabbitMQOptions config1 = new RabbitMQOptions();
        config1.setHost("127.0.0.1");
        config1.setPort(5672);
        client = RabbitMQClient.create(vertx, config1);
        CompletableFuture<Void> latch = new CompletableFuture<>();
        client.start(ar -> {
            if (ar.succeeded()) {
                LOGGER.info(logFront+"[ LogType: info ] [ Message: Start client! ]");
                latch.complete(null);
            } else {
                LOGGER.error(logFront+"[ LogType: error ] [ Can't start client! ] ["+ar.cause()+" ]");
                latch.completeExceptionally(ar.cause());
            }
        });
        ConnectionFactory factory = new ConnectionFactory();
        try {
            channel = factory.newConnection().createChannel();
        } catch (IOException e) {
            LOGGER.error(logFront+"[ LogType: error ] [ Can't start channel! ] ["+ e +" ]");
            e.printStackTrace();
        } catch (TimeoutException e) {
            LOGGER.error(logFront+"[ LogType: error ] [ Can't start channel! ] ["+ e +" ]");
            e.printStackTrace();
        }
        LOGGER.info(logFront+"[ LogType: info ] [ Create client!");
        System.out.println("Connection succeeded!");
        consumeWithManualAck(vertx,client);
        client.stop(stopResult->{
            if (stopResult.succeeded()) {
                LOGGER.info(logFront+"[ LogType: info ] [ Stop client! ]");
                System.out.println("Provider stopped!"+client);
            } else {
                stopResult.cause().printStackTrace();
                LOGGER.error(logFront+"[ LogType: error ] [ Can't stop client! ] ["+ stopResult.cause() +" ]");
                System.out.println("Could not stop MQ client!");
            }
        });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        String logFront ="[ "+config().getString("TestID")+"-"+ dateStr+"-"+config().getString("SeriesID")+" ]  [ "
                +config().getString("host")+"/"+config().getString("AdapterName")+"/"+ProviderAVerticle.class.getName()+" ] [ "+timestamp+" ] ";
        LOGGER.info(logFront+"[ LogType: info ] [ stop vertx! ]");
        super.stop(stopFuture);
    }
    //声明队列并绑定消费者
    public void consumeWithManualAck(Vertx vertx, RabbitMQClient client) {
        String logFront ="[ "+config().getString("TestID")+"-"+ dateStr+"-"+config().getString("SeriesID")+" ]  [ "
                +config().getString("host")+"/"+config().getString("AdapterName")+"/"+ProviderAVerticle.class.getName()+" ] [ "+timestamp+" ] ";
        //声明交换机
        String EXCHANGE_NAME = config().getString("ExchangeName");
        String EXCHANGE_TYPE = config().getString("ExchangeType");
        String RoutingKey = config().getString("OrgID")+"_"+config().getString("HierarChyID");
        this.client.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, false, false, rs -> {
            if (rs != null) {
                LOGGER.info(logFront+"[ LogType: info ] [ Create exchange: "+EXCHANGE_NAME+" ! ]");
                System.out.println("Exchange created !");
            } else {
                LOGGER.error(logFront+"[ LogType: error ] [ Can't create exchange! ]");
                System.out.println("Can't create exchange!");
            }
        });
        //发布消息
        JsonObject message = new JsonObject().put("body", config().getString("Message"));
         client.basicPublish(EXCHANGE_NAME, RoutingKey, message, rs -> {
            if (rs.succeeded()) {
                LOGGER.info(logFront+"[ LogType: info ] [ Message published : "+ message + " to Exchange: "+EXCHANGE_NAME+" ! ]");
                System.out.println("Message published !");
            } else {
                LOGGER.error(logFront+"[ LogType: error ] [ Can't publish message! ] [ "+rs.cause()+" ]");
                rs.cause().printStackTrace();
            }
        });
    }
}