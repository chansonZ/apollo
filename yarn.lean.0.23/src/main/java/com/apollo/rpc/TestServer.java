package com.apollo.rpc;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.EnumSet;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

public class TestServer {

  public static void main(String[] args) throws HadoopIllegalArgumentException, IOException,
      InterruptedException {
    Configuration conf = new Configuration();
    if (args[0].equals("-server")) {
      if (args[1].equals("ssl")) {
        conf.set("hadoop.security.authorization", "true");
        conf.set("hadoop.security.authentication", "TOKEN");
      }
      System.out.println("Testing Slow RPC");
      // create a server with two handlers
      TestTokenSecretManager sm = new TestTokenSecretManager();
      Server server = RPC.getServer(TestProtocol.class, new TestImpl(), "0.0.0.0", 7788, 2, false,
        conf, sm);
      // gen token
      final UserGroupInformation current = UserGroupInformation.getCurrentUser();
      final InetSocketAddress addr = NetUtils.getConnectAddress(server);
      TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current.getUserName()));
      Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(tokenId, sm);
      SecurityUtil.setTokenService(token, addr);
      current.addToken(token);
      // /////////// Write out the container-tokens in the nmPrivate space.
      FileContext lfs = FileContext.getLocalFSFileContext();
      DataOutputStream tokensOutStream = lfs.create(new Path("/tmp/tokens"),
        EnumSet.of(CREATE, OVERWRITE));
      Credentials creds = current.getCredentials();
      creds.writeTokenStorageToStream(tokensOutStream);
      tokensOutStream.flush();
      tokensOutStream.close();

      server.refreshServiceAcl(conf, new TestPolicyProvider());
      server.start();
      server.join();
    } else if (args[0].equals("-client")) {
      conf.set("hadoop.security.authentication", "simple");
      conf.set("hadoop.security.authorization", "false");
      TestProtocol proxy = null;
      InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName(args[1]), 7788);
      // create a client
      // System.getenv().put("HADOOP_TOKEN_FILE_LOCATION", "/tmp/tokens");
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.getLoginUser();
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      System.out.println("Executing with tokens:");
      for (Token<?> token : credentials.getAllTokens()) {
        System.out.println(token);
      }
      for (int i = 0; i < 10; i++) {
        long start = System.nanoTime();
        proxy = RPC.getProxy(TestProtocol.class, TestProtocol.versionID, addr, conf);
        System.out.println(proxy.echo("hi"));
        long end = System.nanoTime();
        System.out.println("spend time:" + (end - start));
      }

    }
  }

  private static final String ACL_CONFIG = "test.protocol.acl";

  private static class TestPolicyProvider extends PolicyProvider {

    @Override
    public Service[] getServices() {
      return new Service[] { new Service(ACL_CONFIG, TestProtocol.class) };
    }
  }

  public static class TestTokenIdentifier extends TokenIdentifier {
    private Text tokenid;
    private Text realUser;
    final static Text KIND_NAME = new Text("test.token");

    public TestTokenIdentifier() {
      this(new Text(), new Text());
    }

    public TestTokenIdentifier(Text tokenid) {
      this(tokenid, new Text());
    }

    public TestTokenIdentifier(Text tokenid, Text realUser) {
      this.tokenid = tokenid == null ? new Text() : tokenid;
      this.realUser = realUser == null ? new Text() : realUser;
    }

    @Override
    public Text getKind() {
      return KIND_NAME;
    }

    @Override
    public UserGroupInformation getUser() {
      if (realUser.toString().isEmpty()) {
        return UserGroupInformation.createRemoteUser(tokenid.toString());
      } else {
        UserGroupInformation realUgi = UserGroupInformation.createRemoteUser(realUser.toString());
        return UserGroupInformation.createProxyUser(tokenid.toString(), realUgi);
      }
    }

    public void readFields(DataInput in) throws IOException {
      tokenid.readFields(in);
      realUser.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      tokenid.write(out);
      realUser.write(out);
    }
  }

  public static class TestTokenSecretManager extends SecretManager<TestTokenIdentifier> {
    @Override
    public byte[] createPassword(TestTokenIdentifier id) {
      return id.getBytes();
    }

    @Override
    public byte[] retrievePassword(TestTokenIdentifier id) throws InvalidToken {
      return id.getBytes();
    }

    @Override
    public TestTokenIdentifier createIdentifier() {
      return new TestTokenIdentifier();
    }
  }

  public static class TestTokenSelector implements TokenSelector<TestTokenIdentifier> {
    @SuppressWarnings("unchecked")
    public Token<TestTokenIdentifier> selectToken(Text service,
        Collection<Token<? extends TokenIdentifier>> tokens) {
      if (service == null) {
        return null;
      }
      for (Token<? extends TokenIdentifier> token : tokens) {
        if (TestTokenIdentifier.KIND_NAME.equals(token.getKind())
            && service.equals(token.getService())) {
          return (Token<TestTokenIdentifier>) token;
        }
      }
      return null;
    }
  }

  @TokenInfo(TestTokenSelector.class)
  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;

    String echo(String value) throws IOException;
  }

  public static class TestImpl implements TestProtocol {
    int fastPingCounter = 0;

    public String echo(String value) throws IOException {
      return value;
    }

    public long getProtocolVersion(String arg0, long arg1) throws IOException {
      return TestProtocol.versionID;
    }

    public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2)
        throws IOException {
      return new ProtocolSignature(TestProtocol.versionID, null);
    }

  }
}
