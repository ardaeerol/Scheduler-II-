public class Surucu {
    public static void main(String[] args) {



        String path1 = "Jobs.txt";
        String path2 = "Dependencies.txt";
        JobScheduler cizelge = new JobScheduler(path1);
        cizelge.setResourcesCount(2); //same as one in hw1
        cizelge.insertDependencies(path2);
//        System.out.println(cizelge.dependencyMap);

        while(cizelge.stillContinues()){ // stillContinues metodu ile job dosyasında bir şey bulunup bulunmadığına bakılır


            cizelge.insertJob(); //insertJob reads a new line from the inputfile, adds a job if necessary
            System.out.println("min-heap\n" + cizelge); // a proper toString method for JobSchedular class
            cizelge.run(); //different from one in hw1
            //printing as a list and printing as a tree
            cizelge.completedJobs(); // prints completed jobs and their completion time
            cizelge.dependencyBlockedJobs(); // prints jobs whose time is up but waits due to its dependency, also prints its dependency
            cizelge.resourceBlockedJobs(); // prints jobs whose time is up but waits due to busy resources
            cizelge.workingJobs(); // prints jobs working on this cycle and its resource
            System.out.println("-------------"+cizelge.timer+"-------------");
        }

            cizelge.runAllRemaining();
            cizelge.allTimeLine();
    }
}