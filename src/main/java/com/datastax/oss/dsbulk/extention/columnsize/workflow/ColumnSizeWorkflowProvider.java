package com.datastax.oss.dsbulk.extention.columnsize.workflow;

import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;

public class ColumnSizeWorkflowProvider implements WorkflowProvider {
  @NonNull
  @Override
  public String getTitle() {
    return "column-size";
  }

  @NonNull
  @Override
  public String getDescription() {
    return "Works just like `unload`, instead of writing the actual data, "
        + "this command writes the column size to the connector."
        + "By default, only the rows with the columns of large size(5MB and more) are written.";
  }

  @NonNull
  @Override
  public Workflow newWorkflow(@NonNull Config config) {
    return new ColumnSizeWorkflow(config);
  }
}
