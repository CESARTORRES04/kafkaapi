package com.kafkaprocon.kafkaapi.model;

public class PedidoEvent {
    private int pedidoId;
    private String cliente;
    private String producto;
    private int cantidad;
    private double precio;
    private String fecha;

    
    
    public PedidoEvent() {
		
	}

	// Constructor, getters y setters
    public PedidoEvent(int pedidoId, String cliente, String producto,
                       int cantidad, double precio, String fecha) {
        this.pedidoId = pedidoId;
        this.cliente  = cliente;
        this.producto = producto;
        this.cantidad = cantidad;
        this.precio   = precio;
        this.fecha    = fecha;
    }
    // getters/setters...

	public int getPedidoId() {
		return pedidoId;
	}

	public void setPedidoId(int pedidoId) {
		this.pedidoId = pedidoId;
	}

	public String getCliente() {
		return cliente;
	}

	public void setCliente(String cliente) {
		this.cliente = cliente;
	}

	public String getProducto() {
		return producto;
	}

	public void setProducto(String producto) {
		this.producto = producto;
	}

	public int getCantidad() {
		return cantidad;
	}

	public void setCantidad(int cantidad) {
		this.cantidad = cantidad;
	}

	public double getPrecio() {
		return precio;
	}

	public void setPrecio(double precio) {
		this.precio = precio;
	}

	public String getFecha() {
		return fecha;
	}

	public void setFecha(String fecha) {
		this.fecha = fecha;
	}
    
}