package main.java.ru.cft;

public class ClientDao {

    private String inn;
    private String name;

    public ClientDao(String inn, String name) {
        this.inn = inn;
        this.name = name;
    }

    public String getInn() {
        return this.inn;
    }

    public void setInn(String inn) {
        this.inn = inn;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
