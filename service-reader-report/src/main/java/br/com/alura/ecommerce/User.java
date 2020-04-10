package br.com.alura.ecommerce;

public class User {
    private final String uuid;
    private final String email;

    public User(String uuid, String email) {
        this.uuid = uuid;
        this.email = email;
    }


    public String getUuid() {
        return uuid;
    }


    public String getReportPath() {
        return "service-reader-report/target/reports/"+uuid+"-report.txt";
    }

    @Override
    public String toString() {
        return "User{" +
                "uuid='" + uuid + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
