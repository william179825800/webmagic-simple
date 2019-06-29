package us.codecraft.webmagic.samples.pipeline;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.dbutils.AsyncQueryRunner;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RegionInsertDBPipline implements Pipeline {
private static final String  ZERO ="000000000000";
    protected static final QueryRunner queryRunner;

    //异步查询对象
    protected static final AsyncQueryRunner asyncQueryRunner;
    //线程池
    private static ExecutorService executorService = Executors.newFixedThreadPool(2);
    static {
        HikariConfig config = new HikariConfig("/db.properties");
        HikariDataSource dataSource = new HikariDataSource(config);
        queryRunner = new QueryRunner(dataSource);

        //初始化异步查询对象
        asyncQueryRunner = new AsyncQueryRunner(executorService, queryRunner);
    }
    @Override
    public void process(ResultItems resultItems, Task task) {

        System.out.println(resultItems);
        Map<String, Object> all = resultItems.getAll();
        Map<String, Object>  provinces = (Map<String, Object> ) all.get("provinces");
        if (MapUtils.isNotEmpty(provinces)){
            System.out.println(provinces);
            String parent = (String) provinces.get("parent");

            Integer level = (Integer) provinces.get("level");
            List<String> provincesList = (List<String>) provinces.get("provinces");
              String sql =  "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '110000', '北京市', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '120000', '天津市', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '130000', '河北省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '140000', '山西省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '150000', '内蒙古自治区', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '210000', '辽宁省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '220000', '吉林省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '230000', '黑龙江省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '310000', '上海市', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '320000', '江苏省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '330000', '浙江省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '340000', '安徽省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '350000', '福建省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '360000', '江西省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '370000', '山东省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '410000', '河南省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '420000', '湖北省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '430000', '湖南省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '440000', '广东省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '450000', '广西壮族自治区', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '460000', '海南省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '500000', '重庆市', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '510000', '四川省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '520000', '贵州省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '530000', '云南省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '540000', '西藏自治区', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '610000', '陕西省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '620000', '甘肃省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '630000', '青海省', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '640000', '宁夏回族自治区', '0', '0', '0'); \n" +
                      "REPLACE INTO `city_area`( `city_code`, `name`, `province_code`, `level`, `is_deleted`) VALUES ( '650000', '新疆维吾尔自治区', '0', '0', '0'); ";
           /* try {
                asyncQueryRunner.batch(sql,null);
            } catch (SQLException e) {
                e.printStackTrace();
            }*/

        }



        Map<String, Object>  citys = (Map<String, Object> ) all.get("citys");
        if (MapUtils.isNotEmpty(citys)){
            String cityparent = (String) citys.get("parent");
            Integer citylevel = (Integer) citys.get("level");
            List<String> citysList = (List<String>) citys.get("citys");
            for (int i = 0; i < citysList.size();i+=2 ) {
       /*         String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                        "( '%s', '%s', '%s', '%s', '0');\n",citysList.get(i).substring(0,6),citysList.get(i+1),cityparent+ZERO.substring(cityparent.length()),citylevel);*/
                String  sql ="REPLACE INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                        "( ?, ?, ?, ?, '0');";//citysList.get(i).substring(0,6),citysList.get(i+1),cityparent+ZERO.substring(cityparent.length()),citylevel);
//                BigInteger id = null;
                try {
                   /* id = */queryRunner.insert(sql, new ScalarHandler<>(), citysList.get(i).substring(0,12), citysList.get(i+1), cityparent+ZERO.substring(cityparent.length()+6),citylevel);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                System.out.println("返回的主键：" /*+ id*/);
                System.out.print(sql);
//                try {
//                    String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
//                    Runtime.getRuntime().exec(cmd).waitFor();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        }

        Map<String, Object>  countys = (Map<String, Object> ) all.get("countys");
        if(MapUtils.isNotEmpty(countys)){
            String countysparent = (String) countys.get("parent");
            Integer countyslevel = (Integer) countys.get("level");
            List<String> countyList = (List<String>) countys.get("countys");
            for (int i = 0; i < countyList.size();i+=2 ) {
             /*   String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                        "( '%s', '%s', '%s', '%s', '0');\n",countyList.get(i).substring(0,6),countyList.get(i+1),countysparent+ZERO.substring(countysparent.length()),countyslevel);
                System.out.print(sql);*/

                String  sql ="REPLACE INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                        "( ?, ?, ?, ?, '0');";
               /* BigInteger id = null;*/
                try {
                    /*id =*/ queryRunner.insert(sql, new ScalarHandler<>(), countyList.get(i).substring(0,12),countyList.get(i+1),countysparent+ZERO.substring(countysparent.length()),countyslevel);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                System.out.println("返回的主键：" +"" /*id*/);
               /* try {
                    String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
                    Runtime.getRuntime().exec(cmd).waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
        }

        Map<String, Object>  towns = (Map<String, Object> ) all.get("towns");
        if(MapUtils.isNotEmpty(towns)){
            String townsparent = (String) towns.get("parent");
            Integer townslevel = (Integer) towns.get("level");
            List<String> townsList = (List<String>) towns.get("towns");
            if (towns.get("tv")==null){
                for (int i = 0; i < townsList.size();i+=2 ) {
                   /* String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                            "( '%s', '%s', '%s', '%s', '0');\n",townsList.get(i).substring(0,9),townsList.get(i+1),townsparent+ZERO.substring(townsparent.length()),townslevel);
                    System.out.print(sql);*/

                    String  sql ="REPLACE INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                            "( ?, ?, ?, ?, '0');";

                     try {
                         queryRunner.insert(sql, new ScalarHandler<>(),townsList.get(i).substring(0,12),townsList.get(i+1),townsparent+ZERO.substring(townsparent.length()),townslevel);
                     } catch (SQLException e) {
                         e.printStackTrace();
                     }

                   /*
                    try {
                        String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
                        Runtime.getRuntime().exec(cmd).waitFor();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }*/
                }
            }else {
                //地区升级之后数据有所变动
                for (int i = 0; i < townsList.size();i+=3 ) {
                   /* String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                            "( '%s', '%s', '%s', '%s', '0');\n",townsList.get(i).substring(0,9),townsList.get(i+1),townsparent+ZERO.substring(townsparent.length()),townslevel);
                    System.out.print(sql);*/

                    String  sql ="REPLACE INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                            "( ?, ?, ?, ?, '0');";

                    try {
                        queryRunner.insert(sql, new ScalarHandler<>(),townsList.get(i).substring(0,12),townsList.get(i+2),townsparent+ZERO.substring(townsparent.length()),townslevel);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                   /*
                    try {
                        String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
                        Runtime.getRuntime().exec(cmd).waitFor();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }*/
                }
            }
        }

//        String parnt = (String) citys.get("parent");
//        citys.get(citys);
//        System.out.println(city);
    }

    public static void main(String[] args) {
        System.out.println(ZERO.substring(5));
    }

}
