package main.java.ru.cft;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

//    private static String aql = "WITH subjects\n" +
//            "            FOR aml_client IN aml_clients\n" +
//            "            FILTER aml_client.src.source == 'SUV' \n" +
//            "                FOR subject IN 1..1 OUTBOUND aml_client is_client\n" +
//            "                FILTER subject.inn == aml_client.inn AND subject.kpp == aml_client.kpp\n" +
//            "                RETURN \n" +
//            "                {\n" +
//            "                    \"id\": aml_client.src.id, \n" +
//            "                    \"kpp\": subject.kpp,\n" +
//            "                    \"clientManager\": aml_client.clientManager, \n" +
//            "                    \"lastLegalCheckDate\": aml_client.lastLegalCheckDate,\n" +
//            "                    \"inn\": aml_client.inn,\n" +
//            "                    \"kio\": aml_client.kio,\n" +
//            "                    \"relationBeginDate\": aml_client.relationBeginDate,\n" +
//            "                    \"govRegDate\": subject.govRegDate,\n" +
//            "                    \"paidCapital\": subject.paidCapital,\n" +
//            "                    \"authorizedCapital\": subject.authorizedCapital\n" +
//            "                }";

//    private static String aql2 = "FOR aml_client IN aml_clients\n" +
//            "FILTER aml_client.inn != '' and aml_client.inn != NULL \n" +
//            "RETURN \n" +
//            "{  \"inn\": aml_client.inn,\n" +
//            "   \"kpp\": aml_client.kpp\n" +
//            " }";

//    private static String test_query = "for c in test_client return c";

    private static String host;
    private static int port;
    private static String user;
    private static String pass;
    private static String path = ".";
    private static String def_base = "AML";
    private static int batch_size = 10000;
    private static String aql;


    private static String getAqlFromFile(String filename) {
        List<String> aqlList = new ArrayList<>();
        try {
            aqlList = Files.readAllLines(Paths.get(filename));
        } catch (IOException ex) {
            System.out.println("Error: Query file " + ex.getMessage() + " not found!");
//            System.out.println(ex.fillInStackTrace());
            System.exit(1);
        }
        return aqlList.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(" "));

    }

    public static void main(String[] args) {

        String help_info = "migration-arangodb-datapump: This Application use for export result of query from ArangoDb into file.\n" +
                "   parameters:\n" +
                "       -h,     --help          Help information\n" +
                "       -s,     --server        ArangoDb server address\n" +
                "       -p,     --port          ArangoDb server port\n" +
                "       -u,     --user          ArangoDb user name\n" +
                "       -pwd,   --pass          ArangoDb password\n" +
                "       -db,    --database      ArandoDb database name\n" +
                "       -b,     --batch         The count lines for export to file (default 10000)\n" +
                "       -xp,    --export-path   Export path for save files (default .)\n" +
                "       -q,     --query-file    File path and name containing aql query (default ./aql)";
        if (args.length == 0) {
            System.out.println(help_info);
            System.exit(0);
        }

        for (int i = 0; i < args.length; i += 2) {
//            log.info("Args:" + args[i]);

            switch (args[i]) {
                case "--server":
                case "-s":
                    host = args[i + 1];
                    break;
                case "--port":
                case "-p":
                    port = Integer.parseInt(args[i + 1]);
                    break;
                case "--user":
                case "-u":
                    user = args[i + 1];
                    break;
                case "--pass":
                case "-pwd":
                    pass = args[i + 1];
                    break;
                case "--path":
                case "-xp":
                    path = args[i + 1];
                    break;
                case "--database":
                case "-db":
                    def_base = args[i + 1];
                    break;
                case "-b":
                case "--batch":
                    batch_size = Integer.parseInt(args[i + 1]);
                    break;
                case "-h":
                case "--help":
                    System.out.println(help_info);
                    System.exit(0);
                case "-q":
                case "--query-file":
                    aql = getAqlFromFile(args[i + 1]);
                    break;
                default:
                    System.out.println("Error: Incorrect parameter:" + args[i]);
                    System.out.println(help_info);
                    System.exit(0);
            }
        }

        if (aql == null) {
            System.out.println("Error: Query not found, please check parameter -q, --query_file");
            System.exit(1);
        }

        System.out.println("---------------------------------------------------------------------------");
        System.out.println("Server:" + host + " port:" + port + " db:" + def_base + " batch:" + batch_size + " path:" + path);
        System.out.println("---------------------------------------------------------------------------");

        ArangoDB.Builder builder = new ArangoDB.Builder();
        builder.host(host, port);
        builder.user(user);
        builder.password(pass);

        ArangoDB arangoDB = builder.build();
        try {
            System.out.println("Connected to ArangoDb, version:" + arangoDB.getVersion().getVersion());
        } catch (ArangoDBException abde) {
            System.out.println("Not connected to arangoDb: " + abde.getErrorMessage());
            System.exit(1);
        }

        System.out.println("Existing database:");
        arangoDB.getDatabases().forEach(System.out::println);
        System.out.println("-------------------------------------------");

        AqlQueryOptions options = new AqlQueryOptions();
        options.batchSize(batch_size);

        ArangoCursor<BaseDocument> cursor = arangoDB.db(def_base).query(aql, null, options, BaseDocument.class);


        System.out.println("aml_clients size:" + arangoDB.db(def_base).collection("aml_clients").count().getCount());
        System.out.println("-------------------------------------------------------------------------------------------");

//        List<ClientDao> clientDaoList = new ArrayList<>();
        AtomicInteger i = new AtomicInteger(1);
        AtomicInteger file_cnt = new AtomicInteger(0);
        AtomicReference<String> filename = new AtomicReference<>();
        List<String> clientList = new ArrayList<>();


        List<String> stringList = new ArrayList<>();
        cursor.forEachRemaining(aDocument -> {
            //log.info("Inn: " + aDocument.getProperties().get("inn") + " name:" + aDocument.getProperties().get("name"));

            aDocument.getProperties().forEach((k, v) -> stringList.add(String.valueOf(v)));


//            System.out.println("str=" + String.join(";",stringList));
//            clientList.add(aDocument.getProperties().get("inn") + ";");
            clientList.add(String.join(";", stringList));
            if (i.get() % batch_size == 0) {
                try {
                    filename.set("Arango_client_" + file_cnt.getAndIncrement());
                    System.out.println("save file:" + filename.get());
                    Files.write(Paths.get(filename.get()), clientList);
                    clientList.clear();


                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            i.getAndIncrement();
            stringList.clear();
        });

        System.exit(0);

//        List<BaseDocument> list = new ArrayList<BaseDocument>();
//        while (cursor.hasNext()) {
//            BaseDocument item = cursor.next();     // <== Exception raised from here !!!!
//            list.add(item);
//            System.out.println("Key: " + item.getAttribute("name"));
//        }


//        try {
//            getClientFroJson("clients_all.json");
//        } catch (IOException | URISyntaxException e) {
//            e.printStackTrace();
//        }


    }


}
