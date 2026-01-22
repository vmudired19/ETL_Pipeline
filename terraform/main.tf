resource "snowflake_warehouse" "de_wh" {
  name                = "DE_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
}
