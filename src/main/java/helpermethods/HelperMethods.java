package helpermethods;

public class HelperMethods {

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.length() == 0 || str.trim().length() == 0;
    }
}
