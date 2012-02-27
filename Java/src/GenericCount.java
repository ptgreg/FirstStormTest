
public class GenericCount<T> implements Comparable {
	public T   Object;
	public int Count;

 @Override
	public int compareTo(java.lang.Object arg0) {
	 	GenericCount<T> obj = (GenericCount<T>) arg0;
	 	if (obj.Count < this.Count)
	 		return -1;
	 	if (obj.Count > this.Count)
	 		return 1;
		return 0;
 	}
}
