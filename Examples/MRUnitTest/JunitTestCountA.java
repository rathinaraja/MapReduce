package MRUnitTest;
import static org.junit.Assert.*;
import org.junit.Test;

public class JunitTestCountA {

	@Test
	public void test() {
		JunitTest test=new JunitTest();
		int output=test.countA("AuthorA");
		assertEquals(2,output);
	}

}
