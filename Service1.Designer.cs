namespace MaddenCoSyncUtility
{
    partial class Service1
    {
	  /// <summary> 
	  /// Required designer variable.
	  /// </summary>
	  private System.ComponentModel.IContainer components = null;

	  /// <summary>
	  /// Clean up any resources being used.
	  /// </summary>
	  /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
	  protected override void Dispose(bool disposing)
	  {
		if (disposing && (components != null))
		{
		    components.Dispose();
		}
		base.Dispose(disposing);
	  }

	  #region Component Designer generated code

	  /// <summary> 
	  /// Required method for Designer support - do not modify 
	  /// the contents of this method with the code editor.
	  /// </summary>
	  private void InitializeComponent()
	  {
		this.components = new System.ComponentModel.Container();
		this.bindingSource1 = new System.Windows.Forms.BindingSource(this.components);
		this.dataSet1 = new MaddenCoSyncUtility.DataSet1();
		((System.ComponentModel.ISupportInitialize)(this.bindingSource1)).BeginInit();
		((System.ComponentModel.ISupportInitialize)(this.dataSet1)).BeginInit();
		// 
		// bindingSource1
		// 
		this.bindingSource1.DataSource = this.dataSet1;
		this.bindingSource1.Position = 0;
		// 
		// dataSet1
		// 
		this.dataSet1.DataSetName = "DataSet1";
		this.dataSet1.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
		// 
		// Service1
		// 
		this.ServiceName = "Service1";
		((System.ComponentModel.ISupportInitialize)(this.bindingSource1)).EndInit();
		((System.ComponentModel.ISupportInitialize)(this.dataSet1)).EndInit();

	  }

	  #endregion

	  private System.Windows.Forms.BindingSource bindingSource1;
	  private DataSet1 dataSet1;
    }
}
