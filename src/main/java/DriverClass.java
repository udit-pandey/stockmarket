import solution.Problem;
import solution.Problem1;
import solution.Problem2;
import solution.Problem3;

import static helpermethods.HelperMethods.isNullOrEmpty;

/**
 * Driver class for the streaming application
 */
public class DriverClass {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new Exception("Please provide broker, topic to subscribe and the problem number for which the application should run!!");
        }
        String broker = args[0];
        String topic = args[1];
        int problemChoice = Integer.parseInt(args[2]);
        Problem problem;

        if (isNullOrEmpty(broker) || isNullOrEmpty(topic)) {
            throw new Exception("Please provide broker and topic to subscribe!!");
        }

        if (problemChoice == 1) {
            problem = new Problem1(broker, topic);
        } else if (problemChoice == 2) {
            problem = new Problem2(broker, topic);
        } else if (problemChoice == 3) {
            problem = new Problem3(broker, topic);
        } else {
            throw new Exception("Invalid problem number !!");
        }
        problem.consumeDataAndAnalyse();
    }
}
