import java.io.*;
import java.util.*;


/*
Arda Erol
201401013
*/

class job{
		
	private int id;
	private int core;
	private int duration;
	private int arrivalTime;
	
	private int enter;
	private int leave;
	
	public job(int id, int duration) {
		this.id = id;
		this.duration = duration;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCore() {
		return core;
	}

	public void setCore(int core) {
		this.core = core;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public int getArrivalTime() {
		return arrivalTime;
	}

	public void setArrivalTime(int arrivalTime) {
		this.arrivalTime = arrivalTime;
	}
	
	public void decrease() {
		this.duration--;
	}
	
	public int getEnter() {
		return this.enter;
	}
	
	public void setEnter(int enter) {
		this.enter = enter;
	}
	
	public int getLeave() {
		return this.leave;
	}
	
	public void setLeave(int leave) {
		this.leave = leave;
	}
	
	public String toString() {
		return this.id + "";
	}
}


public class JobScheduler {
	
	private boolean check = false;
	
	public class core{
		
		int id; 		
		job executing;
		boolean status;		                     
		                       		
		public core(int id) {
			this.id = id;
			
			status = true;
			this.executing = null;
		}
		
		public boolean isAvailable() {
			return status;
		}
		
		public void setStatus(boolean status) {
			this.status = status;
		}
		
		public void take(job executing) {
			this.executing = executing;
			executing.setEnter(timer);
			setStatus(false);
		}
		
		private void release() {
			executing.setLeave(timer-1);
			this.executing = null;
		}
		
		
		public void execute() { 
			if(!done()) executing.decrease();
			
			done();
		}
	
		
		protected boolean done() {
			if(executing.getDuration() == 0) {
				completed.put(executing.getId(), executing);
				release();
				setStatus(true);
								
				return true;
			}
			
			return false;
		}
	}

    public class MinHeap{
    	
    	ArrayList<job> heap = new ArrayList<>();
    	
    	protected int parent(int j) { return (j-1) / 2; }     
    	protected int left(int j) { return 2*j + 1; }
    	protected int right(int j) { return 2*j + 2; }
    	protected boolean hasLeft(int j) { return left(j) < heap.size(); }
    	protected boolean hasRight(int j) { return right(j) < heap.size(); }
    	
    	protected void swap(int i, int j) {
    	    job temp = heap.get(i);
    	    heap.set(i, heap.get(j));
    	    heap.set(j, temp);
    	  }
    	
    	protected void upheap(int j) {
    		while (j > 0) {            
    			int p = parent(j);
    			if ((heap.get(j).getArrivalTime() >= heap.get(p).getArrivalTime())) break; 
    			swap(j, p);
    			j = p;                               
    	    }
    	}
    	
    	protected void downheap(int j) {
    		while (hasLeft(j)) {               
    			int leftIndex = left(j);
    			int smallChildIndex = leftIndex;     
    			if (hasRight(j)) {
    				int rightIndex = right(j);
    		        if (heap.get(leftIndex).getArrivalTime() > heap.get(rightIndex).getArrivalTime())
    		        	smallChildIndex = rightIndex;  
    			}
    		    if (heap.get(smallChildIndex).getArrivalTime() >= heap.get(j).getArrivalTime())
    		    	break;                             
    		     	swap(j, smallChildIndex);
    		     	j = smallChildIndex;                 
    		  	}
    	}
    	
    	protected void heapify() {
    		int startIndex = parent(size()-1);    // start at PARENT of last entry
    		for (int j=startIndex; j >= 0; j--)   // loop until processing the root
    			downheap(j);
    	}
    	
    	public int size() { return heap.size(); }
    	
    	public job min() {
    	    if (heap.isEmpty()) return null;
    	    return heap.get(0);
    	}
    	
    	
    	public job insert(job j)  {
    	    heap.add(j);                      // add to the end of the list
    	    upheap(heap.size() - 1);               // upheap newly added entry
    	    return j;
    	}
    	
    	public job removeMin() {
    	  if (heap.isEmpty()) return null;
    	  job answer = heap.get(0);
    	  swap(0, heap.size() - 1);              // put minimum item at the end
    	  heap.remove(heap.size() - 1);          // and remove it from the list;
    	  downheap(0);                           // then fix new root
    	  return answer;
    	}
    	
    	public String toString() {
    		
    		for(job j: heap)
    			System.out.print(j.getId() + " ");
    		
    		return "";
    	}
    }

    private int line;
    
    public Integer timer;
    public String filePath;
    public MinHeap schedulerTree;
    private HashMap<Integer, job> completed;
    public HashMap<Integer, ArrayList<Integer>> dependencyMap; 
        
    private ArrayList<job> deBlocked;
    private ArrayList<job> reBlocked;  
    private ArrayList<core> processors;

    public JobScheduler(String filePath) {
        this.filePath = filePath;
        
        this.line = detectLine();
        
        this.timer = 0;
        this.completed = new HashMap<>();              
        this.dependencyMap = new HashMap<>();
        
        this.schedulerTree = new MinHeap();
        
        deBlocked = new ArrayList<>();
        reBlocked = new ArrayList<>();
        processors = new ArrayList<>();
        
    }

    public void insertDependencies(String dependencyPath){
    	
    	File file = new File(dependencyPath);
    	
    	if(!file.exists()) { 
    		System.out.println("File not found!");
    		return;
    	}
    	
    	Scanner scan = null;
    	
    	try {
    		scan = new Scanner(new FileInputStream(file));
    		
    		while(scan.hasNext()) {
    			
    			int key = scan.nextInt();
    			scan.skip(" ");
    			int value = scan.nextInt();
    			
    			if(dependencyMap.get(key) != null) dependencyMap.get(key).add(value);
    			
    			else {
    				ArrayList<Integer> ls = new ArrayList<Integer>();
    				ls.add(value);
    				dependencyMap.put(key, ls);
    			}
    		}
    		
    		scan.close();
    	}
    	
    	catch(IOException e) {
    		System.out.println(e.getMessage());
    	}
    }
    
    public boolean stillContinues(){

    	return --line >= 0;
    }

    public void run(){
    	
    	if(check) timer++;
    	
    	for(core c: processors)
    		if(!c.isAvailable())c.execute();
    	
    	while(schedulerTree.size() != 0) {
    		
    		job j = schedulerTree.removeMin();
    		
    		if(!dependencyController(j))deBlocked.add(j);
    		
    		else if(resourceController() == -1)reBlocked.add(j);
    		
    		else {
    			
    			core c = processors.get(resourceController()-1);
    			c.take(j);
    			j.setCore(c.id);
    			
    		}	
    	}			
    	
    	for(job b: deBlocked) 
    		schedulerTree.insert(b);
    	
    	for(job b: reBlocked) 
    		schedulerTree.insert(b);
   	
    }

    public void setResourcesCount(Integer count){

    	for(int i=0; i<count; i++)
    		processors.add(new core(i+1));
    }

    public void insertJob(){
    	
    	job j = read();
    	
    	if(j != null) {
    		j.setArrivalTime(timer+1);
    		schedulerTree.insert(j);
    		
    	}
    	
    	timer++;
    }

    public void completedJobs(){
    	
    	if(completed.size() == 0) {
    		System.out.println("completed jobs");
    		return;
    	}
    	
    	String comp = "completed jobs ";
    	
    	for(Integer i: completed.keySet())
    		comp += (i + ", ");
    	
    	comp = comp.substring(0, comp.length()-2);
    	
    	System.out.println(comp);

    }

    public void dependencyBlockedJobs(){
    	
    	String dep = "dependency blocked jobs ";
    	
    	for(job b: deBlocked) {
    		
    		int id = 0;
    		
    		for(Integer i : dependencyMap.get(b.getId()))
    			if (!completed.keySet().contains(i))id = i;
    		
    		dep += ("(" + b.getId() + ", " + id + ") ");
    	}
    	
    	System.out.println(dep);
    	
    	deBlocked.clear();

    }

    public void resourceBlockedJobs(){
    	
    	if(reBlocked.size() == 0) {
    		System.out.println("resource blocked jobs");
    		return;
    	}
    	
    	String res = "resource blocked jobs (";
    	
    	for(job b: reBlocked)
    		res += (b.getId() + ", ");
    	
    	res = res.substring(0, res.length()-2);
    	
    	res += ")";
    	
    	System.out.println(res);
    	
    	reBlocked.clear();

    }

    public void workingJobs(){
    	
    	String w = "working jobs ";
    	
    	for(core c: processors)
    		if(!c.isAvailable()) {
    			w += ("(" + c.executing.getId() + ", " + c.id + ") ");
    		}
    	
    	System.out.println(w);

    }

    public void runAllRemaining(){
    	
    	check = true;
    	
    	while(schedulerTree.size() != 0 || controller()) run();
    }
    
    public void allTimeLine(){
    	
    	int numCore = processors.size();
    	
    	int [][] timeline = new int[timer][numCore];
    	
    	System.out.println("alltimeline");
    	
    	for(core c: processors)
    		System.out.print("\tR" + c.id);
    	
    	System.out.println();
    	
    	int temp = 1;
    	
    	while(temp != timer) {
    	
    		for(job b: completed.values())     			   			
    			if(b.getEnter() <= temp && b.getLeave() >= temp)    				  				   				
    				timeline[temp-1][b.getCore()-1] = b.getId();
    		
    		temp++;   		
    	}
    	
    	for(int i=0; i<timer-1; i++) {
    		
    		System.out.print(i+1 + "\t");
    		
    		for(int j=0; j<numCore; j++)
    			if(timeline[i][j] != 0) System.out.print(timeline[i][j] + "\t");
    		
    		System.out.println();
    		
    	}
    }

    public String toString(){
    	
    	String str = "";
    	
    	for(job j: schedulerTree.heap)
    		str += (j + " ");
    	
    	return str;

    }
    
    private boolean controller() {
    	
    	for(core c: processors)
    		if(!c.isAvailable())return true;
    	
    	return false;
    }
    
    private int detectLine() {
    	
    	File f = new File(filePath);
    	Scanner scan = null;
    	
    	int line = 0;
    	try {
    		scan = new Scanner(new FileInputStream(f));
    		while(scan.hasNextLine()) {
    			scan.nextLine();
    			line++;
    		}
    	}
    	catch(IOException e) {
    		System.out.println(e.getMessage());
    	}
    	
    	return line;
    }
    
    private job read() {
    	
    	File f = new File(filePath);
    	Scanner scan = null;
    	
    	try {
    		
    		scan = new Scanner(new FileInputStream(f));
    		
    		int temp = timer;
    		while(temp != 0 && scan.hasNextLine()) {
    			scan.nextLine();
    			temp--;
    		}
    		
    		String in = scan.nextLine();
    		if(in.equals("no job"))return null;
    		
    		int id = Integer.parseInt(in.charAt(0) + "");
    		int duration = Integer.parseInt(in.charAt(2) + "");
    		
    		return new job(id, duration);
    		
    	}
    	catch(IOException e) {
    		System.out.println("IOException in read()");
    	}
    	
    	return null;
    }
    
    private boolean dependencyController(job j) {
    	
    	if(dependencyMap.get(j.getId()) == null)return true; //hic bir bagimlilik bulunmuyor
    	
    	ArrayList<Integer> dep = dependencyMap.get(j.getId());
    	for(Integer i: dep)
    		if (!completed.keySet().contains(i))return false;
    	
    	return true;
    	
    }
    
    private int resourceController() {

		for(int i=0; i<processors.size(); i++) 
			
			if(processors.get(i).isAvailable()) 
				return processors.get(i).id;
			
		return -1;

    }
}
