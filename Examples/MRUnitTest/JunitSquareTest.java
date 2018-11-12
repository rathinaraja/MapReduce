package MRUnitTest;
import static org.junit.Assert.*;
import org.junit.Test;
//https://www.youtube.com/watch?v=I8XXfgF9GSc
public class JunitSquareTest {

	@Test
	public void test() {
		JunitTest test=new JunitTest();
		int output=test.square(5);
		assertEquals(26,output);
	}

}
