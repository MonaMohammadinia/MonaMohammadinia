import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Main {
    static final String DB_URL = "jdbc:postgresql://localhost:5433/postgres";
    static final String USER = "postgres";
    static final String PASS = "password";

    public static void main(String[] args) throws Exception {
        try(Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        ) {
            long startedAt = System.nanoTime();
            List<Vendor> vendors = loadAllData(conn);
            System.out.printf("Loaded %d records at %f millis\n", vendors.size(), (System.nanoTime() - startedAt) / 1_000_000f);
            int read = System.in.read();
            System.out.println("Done");
        }

    }

    private static List<Vendor> loadAllData(Connection conn) throws Exception {
        List<Vendor> vendors = new ArrayList<>();
        try(PreparedStatement stmt = conn.prepareStatement("SELECT * FROM minimal_vendor_tbl")) {
            System.out.println("Fetch size: " + stmt.getFetchSize());
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                vendors.add(
                        new Vendor(
                                resultSet.getString("bid"),
                                resultSet.getString("topic"),
                                resultSet.getString("brand"),
                                resultSet.getString("country"),
                                resultSet.getString("partition_key"),
                                resultSet.getTimestamp("timestamp"),
                                resultSet.getString("version"),
                                resultSet.getString("payload"),
                                resultSet.getTimestamp("processed_at"),
                                resultSet.getBytes("protobuf")
                        )
                );
            }
        }

        return vendors;
    }

    private static void insertLotsOfData(Connection conn) throws SQLException {
        StringBuilder q = new StringBuilder();
        q.append("INSERT INTO minimal_vendor_tbl (bid, topic, brand, country, partition_key, timestamp, version, payload, processed_at, protobuf) VALUES ");
        for (int i = 0; i <= 10000; i++) {
            String record = String.format("('%s', 'minimal_vendor', 'fp', 'sg', 'fp-sg', '2022-09-28 15:28:54.000000', 'version', '{\"id\":\"h23u\",\"new_until\":\"2022-07-07T00:00:00Z\",\"minimum_delivery_time\":0,\"minimum_pickup_time\":10,\"rating_score\":0.0,\"rating_count\":0,\"discounts\":[],\"budget_id\":\"0\",\"cuisine_ids\":[],\"characteristics_ids\":[],\"main_cuisine_id\":\"\",\"chain_id\":\"\",\"tags\":[\"TAG_HAS_ONLINE_PAYMENT\",\"TAG_DELIVERY_ENABLED\",\"TAG_TEST\",\"TAG_HAS_DELIVERY_PROVIDER\"],\"vertical_id\":\"groceries\",\"payment_ids\":[\"credit_card\",\"applepay\",\"balance\",\"payment_on_delivery\",\"googlepay\",\"invoice\",\"paypal\",\"reddot_paylah\",\"vodafone\",\"vodafone_cash\"],\"time_zone\":\"Asia/Singapore\",\"deduplicate_chain\":false,\"active\":true,\"accept_voucher\":false,\"points\":0,\"address\":\"Singapore\",\"address_line2\":\"\",\"address_line_other\":\"\",\"post_code\":\"068894\",\"url_key\":\"testvendor1\",\"redirection_url\":\"https://www-st.foodpanda.sg/restaurant/h23u/testvendor1\",\"hero_image\":\"https://images.deliveryhero.io/image/fd-sg/LH/h23u-hero.jpg\",\"hero_listing_image\":\"https://images.deliveryhero.io/image/fd-sg/LH/h23u-listing.jpg\",\"loyalty_program_enabled\":false,\"loyalty_percentage_amount\":0.0,\"legacy_id\":19890,\"is_vat_included\":false,\"customer_type\":\"all\",\"accepts_instructions\":true,\"names\":{\"1\":\"Testvendor1\",\"3\":\"Testvendor1\",\"4\":\"Testvendor1\"},\"descriptions\":{\"1\":\"Testvendor1\",\"3\":\"Testvendor1\",\"4\":\"Testvendor1\"},\"city_name\":\"Singapore\",\"chain_info\":{\"code\":\"\",\"name\":\"\",\"is_accepting_global_vouchers\":false,\"main_vendor_code\":\"\",\"url_key\":\"\"},\"vendor_score\":0,\"identifiable_tags\":[],\"vendor_score_as_double\":0.0,\"vendor_score_variants\":{},\"only_show_open_on_rlp\":false,\"min_preorder_time_offset_in_minutes\":0,\"vertical_ids\":[\"groceries\"],\"vertical_parent\":\"Restaurant\",\"vertical_type\":\"groceries\",\"df_characteristics_ids\":[],\"df_cuisines_ids\":[],\"df_main_cuisine_id\":\"\",\"df_tags_ids\":[],\"is_super_vendor\":false,\"location\":{\"latitude\":1.27853,\"longitude\":103.84916},\"legal_name\":\"\",\"trade_register_number\":\"\",\"delivery_provider\":\"platform_delivery\",\"provides_dine_out_services\":false,\"customer_phone\":\"\"}', '2022-10-07 14:49:54.284587', E'\\\\x0A04683233751214323032322D30372D30375430303A30303A30305A200A4201307204090308017A0967726F63657269657382010B6372656469745F636172648201086170706C6570617982010762616C616E63658201137061796D656E745F6F6E5F64656C6976657279820109676F6F676C65706179820107696E766F69636582010670617970616C82010D726564646F745F7061796C6168820108766F6461666F6E6582010D766F6461666F6E655F636173688A010E417369612F53696E6761706F7265980101B2010953696E6761706F7265CA0106303638383934D2010B7465737476656E646F7231DA013768747470733A2F2F7777772D73742E666F6F6470616E64612E73672F72657374617572616E742F683233752F7465737476656E646F7231E2013B68747470733A2F2F696D616765732E64656C69766572796865726F2E696F2F696D6167652F66642D73672F4C482F683233752D6865726F2E6A7067EA013E68747470733A2F2F696D616765732E64656C69766572796865726F2E696F2F696D6167652F66642D73672F4C482F683233752D6C697374696E672E6A70678002B29B01920203616C6C980201A202100A0131120B5465737476656E646F7231A202100A0133120B5465737476656E646F7231A202100A0134120B5465737476656E646F7231AA02100A0131120B5465737476656E646F7231AA02100A0133120B5465737476656E646F7231AA02100A0134120B5465737476656E646F7231B2020953696E6761706F7265BA0200F2020967726F636572696573FA020A52657374617572616E7482030967726F636572696573B203120930478FDFDB74F43F1191442FA358F65940CA0311706C6174666F726D5F64656C6976657279')", UUID.randomUUID());
            q.append(record);
            if (i != 10000) q.append(",");
        }

        boolean execute = conn.createStatement().execute(q.toString());
        System.out.println(execute);
    }

    private record Vendor(
            String bid,
            String topic,
            String brand,
            String country,
            String pKey,
            Timestamp ts,
            String version,
            String payload,
            Timestamp processedAt,
            byte[] protobuf
    ) {}
}
