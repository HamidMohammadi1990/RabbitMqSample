namespace Consumer.Models;

public class Person
{
    public int Id { get; set; }
    public string ExcelId { get; set; } = default!;
	public string Name { get; set; } = default!;
	public string Email { get; set; } = default!;
	public string Phone { get; set; } = default!;
	public string Address { get; set; } = default!;
	public string Company { get; set; } = default!;
	public string Description { get; set; } = default!;
	public string JobTitle { get; set; } = default!;
}