package io.sustc.service.impl;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //throw new UnsupportedOperationException("12211712");
        return Arrays.asList(12211712);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        String presql = "CREATE DATABASE sustc WITH ENCODING = 'UTF8' LC_COLLATE = 'C' TEMPLATE = template0;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(presql)){
             stmt.executeUpdate();
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String creatdanmu = """
                CREATE TABLE IF NOT EXISTS danmu(
                 BV text not null,
                Mid bigint not null,
                Time float4 not null,
                Content text not null,
                PostTime date not null,
                LikedBy bigint[]
                );""";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(creatdanmu)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String creatidentity = "CREATE TYPE identity AS ENUM ('SUPERUSER','USER');";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(creatidentity)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String creatuser = """
                CREATE TABLE IF NOT EXISTS users(
                Mid int Primary Key,
                Name text not null,
                Sex text,
                Birthday date,
                Level smallint not null default 0,
                Coin int default 0,
                Sign text,
                Identity identity not null,
                Password text not null,
                qq text,
                Wechat text,
                Following bigint[]
                );""";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(creatuser)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String creatvideo = """
                CREATE TABLE IF NOT EXISTS videos(
                BV text not null,
                Title text not null,
                OwnerMid bigint not null,
                OwnerName text not null,
                CommitTime date not null,
                ReviewTime date,
                PublicTime date,
                Duration float4 not null,
                Description text not null,
                Reviewer bigint,
                Likes bigint[],
                Coin bigint[],
                Favorite bigint[],
                ViewerMids bigint[],
                ViewTime float4[]
                );""";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(creatvideo)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        for(DanmuRecord danmu : danmuRecords){
            String bv = danmu.getBv();
            long mid = danmu.getMid();
            float time = danmu.getTime();
            String content = danmu.getContent();
            String postime = danmu.getPostTime().toString();
            String likeby = Arrays.toString(danmu.getLikedBy());
            likeby = likeby.substring(1,likeby.length()-1);
            String insertdanmu = "INSERT INTO danmu (BV,Mid,Time,Content,PostTime,LikedBy) VALUES (\n" +
                                    "'" + bv + "'" + "," +
                                    mid + "," +
                                    time + "," +
                                    "'" + content + "'" + "," +
                                    "'" + postime + "'" + "," +
                                    "'{" + likeby + "}'" +
                                ");";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(insertdanmu)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        //throw new UnsupportedOperationException("TODO: implement your import logic");
        for (UserRecord user : userRecords){
            long mid = user.getMid();
            String name = user.getName();
            String sex = user.getSex();
            String birthday = user.getBirthday();
            int level = user.getLevel();
            int coin = user.getCoin();
            String sign = user.getSign();
            long[] following = user.getFollowing();
            UserRecord.Identity identity = user.getIdentity();
            String password = user.getPassword();
            String qq = user.getQq();
            String wechat = user.getWechat();
            String follow = Arrays.toString(following);
            follow = follow.substring(1,follow.length()-1);
            String insertuser = "INSERT INTO users(Mid,Name,Sex,Birthday,Level,Coin,Sign,Identity,Password,qq,Wechat,Following) values (\n" +
                                    mid + "," +
                                    "'" + name + "'" + "," +
                                    "'" + sex + "'" + "," +
                                    "'" + birthday + "'" + "," +
                                    level + "," +
                                    coin + "," +
                                    "'" + sign + "'" + "," +
                                    "'" + identity + "'" + "," +
                                    "'" + password + "'" + "," +
                                    "'" + qq + "'" + "," +
                                    "'" + wechat + "'" + "," +
                                    "'{" + follow + "}'" +
                                ");";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(insertuser)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        for (VideoRecord video : videoRecords){
            String bv = video.getBv();
            String title = video.getTitle();
            long ownermid = video.getOwnerMid();
            String ownername = video.getOwnerName();
            String committime = video.getCommitTime().toString();
            String reviewtime = video.getReviewTime().toString();
            String publictime = video.getPublicTime().toString();
            float duration = video.getDuration();
            String description = video.getDescription();
            long reviewer = video.getReviewer();
            String like = Arrays.toString(video.getLike());
            like = like.substring(1,like.length()-1);
            String coin = Arrays.toString(video.getCoin());
            coin = coin.substring(1,coin.length()-1);
            String favorite = Arrays.toString(video.getFavorite());
            favorite = favorite.substring(1,favorite.length()-1);
            String viewermids = Arrays.toString(video.getViewerMids());
            viewermids = viewermids.substring(1,viewermids.length()-1);
            String viewtime = Arrays.toString(video.getViewTime());
            viewtime = viewtime.substring(1,viewtime.length()-1);
            String insertvideo = "INSERT INTO videos(BV,Title,OwnerMid,OwnerName,CommitTime,ReviewTime,PublicTime,Duration,Description,Reviewer,Likes,Coin,Favorite,ViewerMids,ViewerTime) VALUES" +
                                "'" + bv + "'" + "," +
                                "'" + title + "'" + "," +
                                ownermid + "," +
                                "'" + ownername + "'" + "," +
                                "'" + committime + "'" + "," +
                                "'" + reviewtime + "'" + "," +
                                "'" + publictime + "'" + "," +
                                duration + "," +
                                "'" + description + "'" + "," +
                                reviewer + "," +
                                "'{" + like + "}'" + "," +
                                "'{" + coin + "}'" + "," +
                                "'{" + favorite + "}'" + "," +
                                "'{" + viewermids + "}'" + "," +
                                "'{" + viewtime + "}'" + "," +
                                ");";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(insertvideo)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
