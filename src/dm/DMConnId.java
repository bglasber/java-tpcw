package dm;

public class DMConnId {
	public String host;
	public int port;

	public DMConnId(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public int hashCode() {
          int hash = 1;
          hash = hash * 17 + port;
          hash = hash * 31 + host.hashCode();
          return hash;
	}

	@Override
	public boolean equals(Object o) {
          // self check
          if (this == o) {
            return true;
		  } else if (o == null) {
            return false;
		  } else if (getClass() != o.getClass()) {
			return false;
		  }

		  DMConnId other = (DMConnId)o;
		  // field comparison
		  return (other.port  == this.port) && (other.host.equals(this.host));
	}

	@Override
	public String toString() {
		return "DMConnId:" + host + ", " + port;
	}

}
