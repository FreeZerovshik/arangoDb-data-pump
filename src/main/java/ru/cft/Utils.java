package main.java.ru.cft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Utils {
    private static File getFileFromResources(String fileName) {

        ClassLoader classLoader = Application.class.getClassLoader();

        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return new File(resource.getFile());
        }

    }

    public static void getClientFroJson(String filename) throws IOException, URISyntaxException {
//        ObjectMapper objectMapper = new ObjectMapper();
//        List<ClientDao> clients = objectMapper.readreadValue(getFileFromResources("test_data.json"), ClientDao.class);
//        List<ClientDao> clientinJson = new ArrayList<>();
        ClassLoader classLoader = Application.class.getClassLoader();

        URL resource = classLoader.getResource(filename);
        ObjectMapper objectMapper = new ObjectMapper();

        assert resource != null;
        try (Stream<String> stream = Files.lines(Paths.get(resource.toURI()))) {

//            stream.forEach(System.out::println);
            stream.forEach(s -> {
// вычитаем необходимые поля из файла
                try {
                    ObjectNode node = objectMapper.readValue(s, ObjectNode.class);
                    System.out.println("name=" + node.get("params").get("client").get("name"));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            });

        }

        System.out.println("test file read!");
    }
    // get file from classpath, resources folder
}
