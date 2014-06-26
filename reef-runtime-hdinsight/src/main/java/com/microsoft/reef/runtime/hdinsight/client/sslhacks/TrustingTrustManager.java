/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.hdinsight.client.sslhacks;

import javax.inject.Inject;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * A TrustManager that trusts all certificates. Basically the "GOTO FAIL" bug implemented in Java.
 * <p/>
 * Hence: DO NOT USE THIS CLASS UNLESS DEBUGGING.
 */
final class TrustingTrustManager implements X509TrustManager {

  @Inject
  public TrustingTrustManager() {
  }

  @Override
  public void checkClientTrusted(
      final X509Certificate[] x509Certificates, final String s) throws CertificateException {
  }

  @Override
  public void checkServerTrusted(
      final X509Certificate[] x509Certificates, final String s) throws CertificateException {
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return new X509Certificate[0];
  }
}
