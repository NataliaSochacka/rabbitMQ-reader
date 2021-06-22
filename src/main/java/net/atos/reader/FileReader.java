package net.atos.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@ToString
public class FileReader {

    private Channel channel;
    private int batch_size;
    private char separator;
    List<String> extensions;

    public List<File> getFilesFromFolder(String folderName) {

        List<File> fileList = null;

        try  {

            fileList = Files.walk(Paths.get(folderName))
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .filter(f -> doesEndWith(f, extensions))
                    .collect(Collectors.toList());
        } catch (IOException e){

            e.printStackTrace();
        }

        return fileList;
    }

    private static boolean doesEndWith(File file, List<String> extensions) {

        boolean result = false;
        for (String fileExtension : extensions) {
            if (file.getName().endsWith(fileExtension)) {
                result = true;
                break;
            }
        }
        return result;
    }

    public void readBatchFromFile(File inFile) {

        List<Map<?, ?>> toSend = new ArrayList<Map<?, ?>>();;

        try {

            CsvSchema csv = CsvSchema.emptySchema().withHeader().withColumnSeparator(separator);
            CsvMapper csvMapper = new CsvMapper();

            MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(inFile);

            int counter = batch_size;

            while (mappingIterator.hasNextValue()) {

                if (counter != 0) {

                    toSend.add(mappingIterator.nextValue());
                    counter -= 1;
                } else {

                    channel.writeAndFlush(toJSON(toSend));
                   // System.out.println(toJSON(toSend));
                   // System.out.println("Sent to server");
                    toSend.clear();
                    counter = batch_size;
                }
            }

            if (!toSend.isEmpty()) {

                channel.writeAndFlush(toJSON(toSend));
               // System.out.println(toJSON(toSend));
                toSend.clear();
            }

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public String toJSON(List<Map<?, ?>> records){

        ObjectMapper mapper = new ObjectMapper();
        String json = "";

        for (Map<?, ?> record : records) {
            try {

                json += mapper.writerWithDefaultPrettyPrinter().writeValueAsString(record);
            } catch (JsonProcessingException e) {

                e.printStackTrace();
            }
        }

        return json;
    }
}