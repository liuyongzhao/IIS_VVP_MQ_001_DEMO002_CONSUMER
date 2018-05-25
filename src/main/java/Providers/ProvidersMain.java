
package Providers;

import io.vertx.core.Vertx;

public class ProvidersMain {
    public static void main(String[] args){
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(ProviderAVerticle.class.getName());
    }
}

