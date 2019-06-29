package us.codecraft.webmagic.samples.pipeline;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RegionPipline implements Pipeline {
private static final String  ZERO ="000000";
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
            try {
                Runtime.getRuntime().exec("cmd.exe /c echo  -- 开始执行 >  D:\\backup\\sql\\region.sql");
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (String p : provincesList) {
                String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES ( 111, '%s', 0, 0, '0');\n",p);
                System.out.print(sql);
                try {
                    String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
                    Runtime.getRuntime().exec(cmd).waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }



        Map<String, Object>  citys = (Map<String, Object> ) all.get("citys");
        if (MapUtils.isNotEmpty(citys)){
            String cityparent = (String) citys.get("parent");
            Integer citylevel = (Integer) citys.get("level");
            List<String> citysList = (List<String>) citys.get("citys");
            for (int i = 0; i < citysList.size();i+=2 ) {
                String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                        "( '%s', '%s', '%s', '%s', '0');\n",citysList.get(i).substring(0,6),citysList.get(i+1),cityparent+ZERO.substring(cityparent.length()),citylevel);
                System.out.print(sql);
                try {
                    String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
                    Runtime.getRuntime().exec(cmd).waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        Map<String, Object>  countys = (Map<String, Object> ) all.get("countys");
        if(MapUtils.isNotEmpty(countys)){
            String countysparent = (String) countys.get("parent");
            Integer countyslevel = (Integer) countys.get("level");
            List<String> countyList = (List<String>) countys.get("countys");
            for (int i = 0; i < countyList.size();i+=2 ) {
                String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                        "( '%s', '%s', '%s', '%s', '0');\n",countyList.get(i).substring(0,6),countyList.get(i+1),countysparent+ZERO.substring(countysparent.length()),countyslevel);
                System.out.print(sql);
                try {
                    String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
                    Runtime.getRuntime().exec(cmd).waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        Map<String, Object>  towns = (Map<String, Object> ) all.get("towns");
        if(MapUtils.isNotEmpty(towns)){
            String townsparent = (String) towns.get("parent");
            Integer townslevel = (Integer) towns.get("level");
            List<String> townsList = (List<String>) towns.get("towns");
            for (int i = 0; i < townsList.size();i+=2 ) {
                String  sql = String.format("INSERT INTO `region`( `code`, `name`, `parent_code`, `level`, `is_deleted`) VALUES " +
                        "( '%s', '%s', '%s', '%s', '0');\n",townsList.get(i).substring(0,9),townsList.get(i+1),townsparent+ZERO.substring(townsparent.length()),townslevel);
                System.out.print(sql);
                try {
                    String cmd =  String.format("cmd.exe /c echo %s >> D:\\backup\\sql\\region.sql",sql);
                    Runtime.getRuntime().exec(cmd).waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        String parnt = (String) citys.get("parent");
//        citys.get(citys);
//        System.out.println(city);
    }

}
