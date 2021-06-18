package net.atos.reader.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;


@Component
@EnableScheduling
public class RabbitClient {

    @Value("${main.directory}")
    private String mainDirectory;

    @Value("#{'${list.of.folders}'.split(',')}")
    private ArrayList<String> folderStringList;

     /*
        CSVReader csvReader = new CSVReader();

        for (String folderName : folderStringList) {

            List<File> fileList = csvReader.getFilesFromFolder(mainDirectory + folderName);

            for (File file : fileList) {

                String msg = csvReader.toJSON(csvReader.readFromFile(file)); // poki co wiadomoscia caly plik
                this.writer.sendMessage(msg);
                logger.info("Sent: " + msg);
            }

        }*/

    @Scheduled(fixedDelayString = "${SEND_INTERVAL}")
    public void run(String... args) throws Exception {

        EventLoopGroup group = new NioEventLoopGroup();

        try {

            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioServerSocketChannel.class)
                    .handler(new RabbitClientInitializer());


        }
        finally {

            group.shutdownGracefully();
        }
    }



}
