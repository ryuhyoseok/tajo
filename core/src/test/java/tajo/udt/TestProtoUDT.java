/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.udt;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TestProtoUDT {

  @Test
  public void testMyFirstUDT() throws DescriptorValidationException {
    addField("name", "Hyunsik Choi", Type.TYPE_STRING);;
    addField("age", 33, Type.TYPE_INT32);
    Message message = build("Succeeded");
    System.out.println(message.toByteArray().length);
    System.out.println(message);
  }

  Map<String, Object> values = new HashMap<>();
  DescriptorProtos.DescriptorProto.Builder desBuilder
      = DescriptorProtos.DescriptorProto.newBuilder();
  private int i = 1;
  private void addField(String fieldName , Object fieldValue, FieldDescriptorProto.Type fieldType ) {

    Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName(fieldName).setNumber(i).setType(fieldType);
    desBuilder.addField((FieldDescriptorProto) fieldBuilder.build());
    values.put(fieldName, fieldValue);
    i+=1;
  }

  private Message build(String messageName)
      throws DescriptorValidationException {
    desBuilder.setName(messageName);
    DescriptorProto dsc = desBuilder.build();
    FileDescriptorProto fileDescP = DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(dsc).build();
    Descriptors.FileDescriptor [] fileDecs = new Descriptors.FileDescriptor[0];
    FileDescriptor dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescP, fileDecs);
    Descriptor msgDescriptor = dynamicDescriptor.findMessageTypeByName(messageName);
    DynamicMessage.Builder dmBuilder = DynamicMessage.newBuilder(msgDescriptor);

    for (Entry<String, Object> entry : values.entrySet()) {
      dmBuilder.setField(msgDescriptor.findFieldByName(entry.getKey()), entry.getValue());
    }
    return dmBuilder.build();
  }
}
