package com.microsoft.reef.examples.nggroup.bgd.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * The memory used for each Evaluator. In MB.
 */
@NamedParameter(short_name = "memory", default_value = "1024", doc = "The memory used for each Evaluator. In MB.")
public final class EvaluatorMemory implements Name<Integer> {
}
