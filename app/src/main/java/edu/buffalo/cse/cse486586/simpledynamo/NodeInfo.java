package edu.buffalo.cse.cse486586.simpledynamo;

public class NodeInfo {
    private String pre_1;
    private String pre_2;
    private String suc_1;
    private String suc_2;

    public String getMy_no() {
        return my_no;
    }

    public void setMy_no(String my_no) {
        this.my_no = my_no;
    }

    private String my_no;
    public String getPre_1() {
        return pre_1;
    }

    public void setPre_1(String pre_1) {
        this.pre_1 = pre_1;
    }

    public String getPre_2() {
        return pre_2;
    }

    public void setPre_2(String pre_2) {
        this.pre_2 = pre_2;
    }

    public String getSuc_1() {
        return suc_1;
    }

    public void setSuc_1(String suc_1) {
        this.suc_1 = suc_1;
    }

    public String getSuc_2() {
        return suc_2;
    }

    public void setSuc_2(String suc_2) {
        this.suc_2 = suc_2;
    }

    public String getNode_no() {
        return node_no;
    }

    public void setNode_no(String node_no) {
        this.node_no = node_no;
    }

    private String node_no;
    public NodeInfo(String my_no,String pre_1, String pre_2, String suc_1, String suc_2){
        this.my_no=my_no;
        this.pre_1=pre_1;
        this.pre_2=pre_2;
        this.suc_1=suc_1;
        this.suc_2=suc_2;
    }

    @Override
    public String toString() {
        String return_string = this.my_no+" "+this.pre_1+" "+this.pre_2+" "+this.suc_1+" "+this.suc_2;
        return return_string;
    }
}
