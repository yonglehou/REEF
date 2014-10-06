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

#include "Clr2JavaImpl.h"

namespace Microsoft {
  namespace Reef {
    namespace Driver {
      namespace Bridge {
        ref class ManagedLog {
          internal:
            static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
        };
        ContextMessageClr2Java::ContextMessageClr2Java(JNIEnv *env, jobject jobjectContextMessage) {
          ManagedLog::LOGGER->LogStart("ContextMessageClr2Java::ContextMessageClr2Java");

          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectContextMessage = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectContextMessage));
          jclass jclassContextMessage = env->GetObjectClass (_jobjectContextMessage);

          jfieldID jidId = env->GetFieldID(jclassContextMessage, "contextMessageId", "Ljava/lang/String;");
          jfieldID jidSourceId = env->GetFieldID(jclassContextMessage, "messageSourceId", "Ljava/lang/String;");
          jfieldID jidMessage = env->GetFieldID(jclassContextMessage, "message", "()[B");

          _jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectContextMessage, jidId)));
          _jstringSourceId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectContextMessage, jidSourceId)));
          _jarrayMessage = reinterpret_cast<jbyteArray>(env->NewGlobalRef(env->GetObjectField(_jobjectContextMessage, jidMessage)));

          ManagedLog::LOGGER->LogStop("ContextMessageClr2Java::ContextMessageClr2Java");
        }

        String^ ContextMessageClr2Java::GetId() {
          ManagedLog::LOGGER->Log("ContextMessageClr2Java::GetId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringId);
        }

        String^ ContextMessageClr2Java::GetMessageSourceId() {
          ManagedLog::LOGGER->Log("ContextMessageClr2Java::GetMessageSourceId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringSourceId);
        }

        array<byte>^ ContextMessageClr2Java::Get() {
          ManagedLog::LOGGER->Log("ContextMessageClr2Java::Get");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedByteArrayFromJavaByteArray(env, _jarrayMessage);
        }

        void ContextMessageClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("ContextMessageClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectContextMessage);
        }
      }
    }
  }
}