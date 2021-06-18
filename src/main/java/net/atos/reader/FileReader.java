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
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@ToString
public class FileReader {

    private Channel channel;

    @Value("${BATCH_SIZE}")
    public int BATCH_SIZE;

    public List<File> getFilesFromFolder(String folderName) {

        List<File> fileList = null;

        try  {

            fileList = Files.walk(Paths.get(folderName))
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .collect(Collectors.toList());
        } catch (IOException e){

            e.printStackTrace();
        }

        // Dodac sprawdzanie rozszerzen plikow (zeby mialy csv i tsv)

        return fileList;
    }

    public List<Map<?, ?>> readFromFile(File inFile) {

        List<Map<?, ?>> list = null;

        try {

            CsvSchema csv = CsvSchema.emptySchema().withHeader();
            CsvMapper csvMapper = new CsvMapper();

            MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(inFile);

            list = mappingIterator.readAll();

        } catch(Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    public void readBatchFromFile(File inFile) {

        List<Map<?, ?>> toSend = null;

        try {

            CsvSchema csv = CsvSchema.emptySchema().withHeader();
            CsvMapper csvMapper = new CsvMapper();

            MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(inFile);

            int counter = BATCH_SIZE;

            while (mappingIterator.hasNextValue()) {

                if (counter != 0) {

                    toSend.add(mappingIterator.nextValue());
                    counter -= 1;
                }

                channel.write(toJSON(toSend));
                toSend.clear();
                counter = BATCH_SIZE;
            }

            if (!toSend.isEmpty()) {

                channel.write(toJSON(toSend));
                toSend.clear();
            }

           // channel.flush();

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
