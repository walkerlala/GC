package cn.lasagna.www.classifier;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by walkerlala on 16-11-15.
 */
public class Record {
    private int page_id;
    private String page_url;
    private String domain_name;
    private Set<String> sublinks;
    private String title;
    private String normal_content;
    private String emphasized_content;
    private String keywords;
    private String description;
    private String text;
    private double PR_score;
    private long ad_NR;
    private String tag;

    public Record() {
        this.page_id = 0;
        this.page_url = null;
        this.domain_name = null;
        this.sublinks = new HashSet<>();
        this.title = null;
        this.normal_content = null;
        this.emphasized_content = null;
        this.keywords = null;
        this.description = null;
        this.text = null;
        this.PR_score = 0.0;
        this.ad_NR = 0L;
        this.tag = null;
    }

    public void setPage_id(int id) {
        this.page_id = id;
    }

    public void setPage_url(String url) {
        this.page_url = url;
    }

    public void setDomain_name(String domain) {
        this.domain_name = domain;
    }

    public void setSublinks(String sublinks) {
        String[] sls = sublinks.split(";");
        for (String s : sls) {
            this.sublinks.add(s);
        }
    }

    public void setSublinks(Collection<String> sublinks) {
        this.sublinks = (Set<String>) sublinks;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setNormal_content(String normal_content) {
        this.normal_content = normal_content;
    }

    public void setEmphasized_content(String emphasized_content) {
        this.emphasized_content = emphasized_content;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setPR_score(double score) {
        this.PR_score = score;
    }

    public void setAd_NR(long ad_nr) {
        this.ad_NR = ad_nr;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getPage_id() {
        return this.page_id;
    }

    public String getPage_url() {
        return this.page_url;
    }

    public String getDomain_name() {
        return this.domain_name;
    }

    public Set<String> getSublinks() {
        return this.sublinks;
    }

    public String getTitle() {
        return this.title;
    }

    public String getNormal_content() {
        return this.normal_content;
    }

    public String getEmphasized_content() {
        return this.emphasized_content;
    }

    public String getKeywords() {
        return this.keywords;
    }

    public String getDescription() {
        return this.description;
    }

    public String getText() {
        return this.text;
    }

    public double getPR_score() {
        return this.PR_score;
    }

    public long getAd_NR() {
        return this.ad_NR;
    }

    public String getTag() {
        return this.tag;
    }

    public void putAll(Record rd) {
        this.setDescription(rd.getDescription());
        this.setKeywords(rd.getKeywords());
        this.setText(rd.getText());
        this.setAd_NR(rd.getAd_NR());
        this.setDomain_name(rd.getDomain_name());
        this.setEmphasized_content(rd.getEmphasized_content());
        this.setNormal_content(rd.getNormal_content());
        this.setPage_id(rd.getPage_id());
        this.setPage_url(rd.getPage_url());
        this.setPR_score(rd.getPR_score());
        this.setSublinks(rd.getSublinks());
        this.setTag(rd.getTag());
        this.setTitle(rd.getTitle());
    }

}
