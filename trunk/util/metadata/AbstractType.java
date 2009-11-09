package metadata;

/**
 * 
 * AbstractType defines common behavior for the types available
 * 
 * @author myahya
 * 
 */
public abstract class AbstractType implements Type {

	@Override
	public int getLength() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getName() {
		return this.getClass().getName();
	}

}
