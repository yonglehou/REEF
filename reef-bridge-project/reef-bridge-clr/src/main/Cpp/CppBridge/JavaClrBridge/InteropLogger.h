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

#include "InteropUtil.h"
#include "com_microsoft_reef_javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#using "clrhandler.dll"

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace Microsoft::Reef::Interop;

namespace Microsoft {
  namespace Reef {
    namespace Interop {
      public ref class InteropLogger : public ILogger {
          jobject _jobjectInteropLogger;
          jclass  _jclassInteropLogger;
          jmethodID _jmidLog;
          JNIEnv* _env;

        public:
          InteropLogger (JNIEnv* env, jobject jobjectInteropLogger);
          virtual void Log(TraceLevel traceLevel, String^ message );
      };
    }
  }
}