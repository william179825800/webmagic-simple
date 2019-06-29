package us.codecraft.webmagic.samples;

import org.apache.commons.collections.CollectionUtils;
import org.jsoup.nodes.Document;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.samples.pipeline.RegionInsertDBPipline;
import us.codecraft.webmagic.samples.pipeline.RegionPipline;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.Selectable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author code4crafter@gmail.com <br>
 */
public class SinaBlogProcessor implements PageProcessor {

//    public static final String URL_LIST = "http://blog\\.sina\\.com\\.cn/s/articlelist_1487828712_0_\\d+\\.html";
    public static final String URL_LIST ="http://www\\.stats\\.gov\\.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/((\\d{2}/)*)\\d{2,}\\.html";
    public static final String URL_CITY ="http://www\\.stats\\.gov\\.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/\\d{2,}\\.html";
    public static final String URL_TOWN ="http://www\\.stats\\.gov\\.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/((\\d{2}/){1})\\d{2,}\\.html";
    public static final String URL_STREET ="http://www\\.stats\\.gov\\.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/((\\d{2}/){2})\\d{2,}\\.html";
    public static final String URL_VILLAGE ="http://www\\.stats\\.gov\\.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/((\\d{2}/){3})\\d{2,}\\.html";
    public static final String URL_INDEX="http://www\\.stats\\.gov\\.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/index\\.html";

    public static final String URL_POST = "http://blog\\.sina\\.com\\.cn/s/blog_\\w+\\.html";

    private Site site = Site
            .me()
            .setDomain("www.stats.gov.cn")
            .setSleepTime(3000)
            .setRetryTimes(3)
            .setTimeOut(6000)
            .setCharset("GBK")
            .setUserAgent(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.65 Safari/537.31");


    @Override
    public void process(Page page) {
        Html html = page.getHtml();
            page.addTargetRequests(page.getHtml().css(".provincetr a").links().all());
           // page.addTargetRequests(page.getHtml().xpath("//div[@class=\"articleList\"]").links().regex(URL_POST)  .all());
          //  page.addTargetRequests(page.getHtml().links().regex(URL_LIST).all());
            System.out.println(page.getTargetRequests());
            System.out.println(html);
            //列表页
            if (page.getUrl().regex(URL_INDEX).match()) {
                //省
            List<String> text = page.getHtml().css(".provincetr a", "text").all();
            Map<String,Object> map =new HashMap<String, Object>();
            map.put("provinces",text);
            map.put("parent","0");
            map.put("level",0);
            page.putField("provinces", map);
        }else if (page.getUrl().regex(URL_CITY).match()){
            // 市
            page.addTargetRequests(page.getHtml().css(".citytr a").links().all());
            Selectable url = page.getUrl();
            System.out.println("市"+url);
            String str =url.toString();
            Map<String,Object> map =new HashMap<String, Object>();
            int beginIndex = url.toString().lastIndexOf("/");
            String parent = str.substring(str.lastIndexOf("/")).split("/")[1].split("\\.")[0];
            map.put("parent",parent);
            map.put("level",1);
            map.put("citys",page.getHtml().css(".citytr a","text").all());
            page.putField("citys",map);
        }
        else if (page.getUrl().regex(URL_TOWN).match()){
            //县
            Selectable url = page.getUrl();
            Map<String,Object> map =new HashMap<String, Object>();
            int beginIndex = url.toString().lastIndexOf("/");
            String str =url.toString();
            String parent = str.substring(str.lastIndexOf("/")).split("/")[1].split("\\.")[0];
            map.put("parent",parent);
            map.put("level",2);
            map.put("countys",page.getHtml().css(".countys a","text").all());
            if (CollectionUtils.isEmpty((ArrayList)map.get("countys"))){
                map.put("countys",page.getHtml().css(".towntr a","text").all());
                page.addTargetRequests(page.getHtml().css(".towntr a").links().all());
            }else{
                page.addTargetRequests(page.getHtml().css(".countytr a").links().all());
            }

            page.putField("countys",map);
            System.out.println("县"+url);

        }else if(page.getUrl().regex(URL_STREET).match()){
            //镇
            Selectable url = page.getUrl();
            Map<String,Object> map =new HashMap<String, Object>();
            int beginIndex = url.toString().lastIndexOf("/");
            String str =url.toString();
            String parent = str.substring(str.lastIndexOf("/")).split("/")[1].split("\\.")[0];
            map.put("parent",parent);
            map.put("level",3);
            map.put("towns",page.getHtml().css(".towntr a","text").all());
            if (CollectionUtils.isEmpty((ArrayList)map.get("towns"))){
                map.put("towns",page.getHtml().css(".villagetr td","text").all());
                map.put("tv",Boolean.TRUE);
            }
            //page.addTargetRequests(page.getHtml().css(".towntr a").links().all());
            page.putField("towns",map);
            System.out.println( "镇："+page.getUrl());
        }

       /* else if(page.getUrl().regex(URL_VILLAGE).match()){
            //村
            page.addTargetRequests(page.getHtml().css(".towntr a").links().all());
            page.putField("towns",page.getHtml().css(".towntr a","text").all());
            System.out.println( "村："+page.getUrl());
        }*/
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new SinaBlogProcessor())
                .addPipeline(new RegionInsertDBPipline())
                .addUrl("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/index.html")
                .run();
    }
}
