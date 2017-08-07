package hadoop.hbase.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Package hadoop.hbase.mapreduce
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/6 2:52
 * @Email elonyong@163.com
 */
public class ProductModel implements WritableComparable<ProductModel> {
    private String id;
    private String name;
    private String price;

    public ProductModel() {
    }

    public ProductModel(String id, String name, String price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeUTF(this.name);
        dataOutput.writeUTF(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.price = dataInput.readUTF();
    }

    @Override
    public int compareTo(ProductModel o) {
        if (this == o) {
            return 0;
        }
        int tmp = this.id.compareTo(o.id);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.name.compareTo(o.name);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.price.compareTo(o.price);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProductModel that = (ProductModel) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return price != null ? price.equals(that.price) : that.price == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (price != null ? price.hashCode() : 0);
        return result;
    }
}
