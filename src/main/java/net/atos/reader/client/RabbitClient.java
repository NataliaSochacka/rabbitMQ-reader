package net.atos.reader.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.atos.reader.FileReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


@Component
@EnableScheduling
public class RabbitClient {

    @Value("${main.directory}")
    private String mainDirectory;

    @Value("#{'${list.of.folders}'.split(',')}")
    private ArrayList<String> folderStringList;

    @Value("#{'${extensions}'.split(',')}")
    private ArrayList<String> extensions;

    @Value("${BATCH_SIZE}")
    private int BATCH_SIZE;

    @Value("${SEPARATOR}")
    private char SEPARATOR;

     public void readFolders(FileReader fileReader) {

         for (String folderName : folderStringList) {

             List<File> fileList = fileReader.getFilesFromFolder(mainDirectory + folderName);

             for (File file : fileList) {

                 fileReader.readBatchFromFile(file);
             }
         }
     }

    @Scheduled(fixedDelayString = "${SEND_INTERVAL}")
    public void run() throws Exception {

        EventLoopGroup group = new NioEventLoopGroup();

        try {

            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new RabbitClientInitializer());

            Channel channel = bootstrap.connect("localhost", 8000).channel();
            FileReader reader = new FileReader(channel, BATCH_SIZE, SEPARATOR, extensions);

            readFolders(reader);
        }
        finally {

            group.shutdownGracefully();
        }
    }
}
