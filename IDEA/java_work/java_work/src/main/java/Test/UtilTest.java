package Test;

public class UtilTest {
    public static void main(String[] args) {
        Util util = new Util();
        util.crtComCL("comCS","comCL","testDomain");
        util.crtMainCL("mainCS","mainCL","testDomain");
        util.crtSubCL("subCS","subCL1","testDomain");

    }
}
