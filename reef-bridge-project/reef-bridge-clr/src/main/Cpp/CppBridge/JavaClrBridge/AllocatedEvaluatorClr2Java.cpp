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

using namespace JavaClrBridge;

namespace Microsoft {
  namespace Reef {
    namespace Driver {
      namespace Bridge {
        ref class ManagedLog {
          internal:
            static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
        };

        AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator) {

          ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java");

          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectAllocatedEvaluator = reinterpret_cast<jobject>(env->NewGlobalRef(jallocatedEvaluator));

          jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
          jfieldID jidEvaluatorId = env->GetFieldID(jclassAllocatedEvaluator, "evaluatorId", "Ljava/lang/String;");
          _jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectAllocatedEvaluator, jidEvaluatorId)));

          jfieldID jidNameServerInfo = env->GetFieldID(jclassAllocatedEvaluator, "nameServerInfo", "Ljava/lang/String;");
          _jstringNameServerInfo = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectAllocatedEvaluator, jidNameServerInfo)));

          ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java");
        }

        void AllocatedEvaluatorClr2Java::SubmitContext(String^ contextConfigStr) {
          ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContext");
          JNIEnv *env = RetrieveEnv(_jvm);
          jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
          jmethodID jmidSubmitContext = env->GetMethodID(jclassAllocatedEvaluator, "submitContextString", "(Ljava/lang/String;)V");

          if (jmidSubmitContext == NULL) {
            ManagedLog::LOGGER->Log("jmidSubmitContext is NULL");
            return;
          }
          env -> CallObjectMethod(
            _jobjectAllocatedEvaluator,
            jmidSubmitContext,
            JavaStringFromManagedString(env, contextConfigStr));
          ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContext");
        }

        void AllocatedEvaluatorClr2Java::SubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr) {
          ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContextAndTask");
          JNIEnv *env = RetrieveEnv(_jvm);
          jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
          jmethodID jmidSubmitContextAndTask = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndTaskString", "(Ljava/lang/String;Ljava/lang/String;)V");

          if (jmidSubmitContextAndTask == NULL) {
            ManagedLog::LOGGER->Log("jmidSubmitContextAndTask is NULL");
            return;
          }
          env -> CallObjectMethod(
            _jobjectAllocatedEvaluator,
            jmidSubmitContextAndTask,
            JavaStringFromManagedString(env, contextConfigStr),
            JavaStringFromManagedString(env, taskConfigStr));
          ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContextAndTask");
        }

        void AllocatedEvaluatorClr2Java::SubmitContextAndService(String^ contextConfigStr, String^ serviceConfigStr) {
          ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContextAndService");
          JNIEnv *env = RetrieveEnv(_jvm);
          jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
          jmethodID jmidSubmitContextAndService = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndServiceString", "(Ljava/lang/String;Ljava/lang/String;)V");

          if (jmidSubmitContextAndService == NULL) {
            ManagedLog::LOGGER->Log("jmidSubmitContextAndService is NULL");
            return;
          }
          env -> CallObjectMethod(
            _jobjectAllocatedEvaluator,
            jmidSubmitContextAndService,
            JavaStringFromManagedString(env, contextConfigStr),
            JavaStringFromManagedString(env, serviceConfigStr));
          ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContextAndService");
        }

        void AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask(String^ contextConfigStr, String^ serviceConfigStr, String^ taskConfigStr) {
          ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask");
          JNIEnv *env = RetrieveEnv(_jvm);
          jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
          jmethodID jmidSubmitContextAndServiceAndTask = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndServiceAndTaskString", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");

          if (jmidSubmitContextAndServiceAndTask == NULL) {
            ManagedLog::LOGGER->Log("jmidSubmitContextAndServiceAndTask is NULL");
            return;
          }
          env -> CallObjectMethod(
            _jobjectAllocatedEvaluator,
            jmidSubmitContextAndServiceAndTask,
            JavaStringFromManagedString(env, contextConfigStr),
            JavaStringFromManagedString(env, serviceConfigStr),
            JavaStringFromManagedString(env, taskConfigStr));
          ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask");
        }

        void AllocatedEvaluatorClr2Java::OnError(String^ message) {
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectAllocatedEvaluator);
        }

        void AllocatedEvaluatorClr2Java::Close() {
          ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::Close");
          JNIEnv *env = RetrieveEnv(_jvm);
          jclass jclassAllocatedEvaluator = env->GetObjectClass (_jobjectAllocatedEvaluator);
          jmethodID jmidClose = env->GetMethodID(jclassAllocatedEvaluator, "close", "()V");

          if (jmidClose == NULL) {
            ManagedLog::LOGGER->Log("jmidClose is NULL");
            return;
          }
          env -> CallObjectMethod(
            _jobjectAllocatedEvaluator,
            jmidClose);
          ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::Close");
        }

        String^ AllocatedEvaluatorClr2Java::GetId() {
          ManagedLog::LOGGER->Log("AllocatedEvaluatorClr2Java::GetId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringId);
        }

        String^ AllocatedEvaluatorClr2Java::GetNameServerInfo() {
          ManagedLog::LOGGER->Log("AllocatedEvaluatorClr2Java::GetNameServerInfo");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringNameServerInfo);
        }

        IEvaluatorDescriptor^ AllocatedEvaluatorClr2Java::GetEvaluatorDescriptor() {
          ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::GetEvaluatorDescriptor");
          return CommonUtilities::RetrieveEvaluatorDescriptor(_jobjectAllocatedEvaluator, _jvm);
        }
      }
    }
  }
}